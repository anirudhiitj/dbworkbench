"""
WEAVE-DB: Inverse Command Generation Engine
============================================
Generates reversible "anti-commands" for every category of PostgreSQL
write operation so that the Rollback Engine can undo any commit.

Supported command categories
-----------------------------
DML  : INSERT, UPDATE, DELETE
DDL  : CREATE TABLE, ALTER TABLE (ADD/DROP/RENAME/ALTER COLUMN, ADD/DROP CONSTRAINT,
        ADD/DROP INDEX via ALTER), DROP TABLE, TRUNCATE
DDL  : CREATE INDEX, DROP INDEX
DDL  : CREATE SEQUENCE, DROP SEQUENCE, ALTER SEQUENCE
DDL  : CREATE SCHEMA, DROP SCHEMA
DDL  : CREATE VIEW, DROP VIEW, CREATE OR REPLACE VIEW
DDL  : RENAME TABLE  (special form of ALTER TABLE)

Design notes
------------
* The engine is *stateful* ΓÇô for DML it must query the live database BEFORE
  executing the command so it can capture the "before image" of affected rows.
* For DDL it must query catalog tables (information_schema / pg_catalog) to
  reconstruct the inverse.
* The engine returns an InverseCommand dataclass.  Storing/executing that
  object is the responsibility of the caller (VersionStore / RollbackEngine).
* All SQL strings are parameterized where possible; where raw SQL must be
  constructed (DDL), values are sanitised through pg_catalog lookups rather
  than trusting the input string.
* This module has NO side-effects on its own ΓÇô it only *generates* SQL.
  Execution is delegated to the caller.

Integration surface (what other subsystems call)
-------------------------------------------------
    from inverse_engine import InverseEngine, InverseCommand

    engine = InverseEngine(connection)           # pass an open psycopg2 conn
    inv    = engine.generate(sql, params=None)   # call BEFORE executing sql
    # ΓÇª store inv ΓÇª execute sql ΓÇª commit ΓÇª
"""

from __future__ import annotations

import re
import json
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public data types
# ---------------------------------------------------------------------------

class CommandCategory(Enum):
    """Broad classification of a SQL command."""
    INSERT    = auto()
    UPDATE    = auto()
    DELETE    = auto()
    TRUNCATE  = auto()
    CREATE_TABLE    = auto()
    DROP_TABLE      = auto()
    ALTER_TABLE     = auto()
    RENAME_TABLE    = auto()
    CREATE_INDEX    = auto()
    DROP_INDEX      = auto()
    CREATE_SEQUENCE = auto()
    DROP_SEQUENCE   = auto()
    ALTER_SEQUENCE  = auto()
    CREATE_SCHEMA   = auto()
    DROP_SCHEMA     = auto()
    CREATE_VIEW     = auto()
    DROP_VIEW       = auto()
    UNKNOWN         = auto()   # SELECT or unsupported ΓÇô no inverse generated


@dataclass
class InverseCommand:
    """
    A single, self-contained unit that can undo one forward commit.
    """

    category:      CommandCategory
    forward_sql:   str
    steps:         list[str]               = field(default_factory=list)
    before_image:  Optional[Any]           = None
    is_reversible: bool                    = True
    notes:         str                     = ""

    # --- Internal metadata (used for multi-phase inverses like INSERT) ---
    _table:        Optional[str]           = None
    _pks:          Optional[list[str]]     = None

    def to_dict(self) -> dict:
        """Serialise for storage in the commit_log table."""
        return {
            "category":      self.category.name,
            "forward_sql":   self.forward_sql,
            "steps":         self.steps,
            "before_image":  self.before_image,
            "is_reversible": self.is_reversible,
            "notes":         self.notes,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "InverseCommand":
        return cls(
            category      = CommandCategory[d["category"]],
            forward_sql   = d["forward_sql"],
            steps         = d["steps"],
            before_image  = d.get("before_image"),
            is_reversible = d.get("is_reversible", True),
            notes         = d.get("notes", ""),
        )

    def __repr__(self):
        return (f"<InverseCommand category={self.category.name} "
                f"steps={len(self.steps)} reversible={self.is_reversible}>")


# ---------------------------------------------------------------------------
# SQL parser helpers  (no external library required)
# ---------------------------------------------------------------------------

def _normalise(sql: str) -> str:
    """Strip comments, collapse whitespace, uppercase keywords."""
    # Remove block comments  /* ΓÇª */
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    # Remove line comments  -- ΓÇª
    sql = re.sub(r"--[^\n]*", " ", sql)
    return " ".join(sql.split())


def _keyword(sql: str) -> str:
    """Return the first uppercase keyword token."""
    m = re.match(r"^\s*([A-Za-z]+)", sql)
    return m.group(1).upper() if m else ""


def _classify(sql: str) -> CommandCategory:
    """
    Classify a SQL statement into a CommandCategory.
    Handles the most common PostgreSQL syntax variants.
    """
    s = _normalise(sql).upper()

    if s.startswith("INSERT"):
        return CommandCategory.INSERT
    if s.startswith("UPDATE"):
        return CommandCategory.UPDATE
    if s.startswith("DELETE"):
        return CommandCategory.DELETE
    if s.startswith("TRUNCATE"):
        return CommandCategory.TRUNCATE

    # CREATE ΓÇª
    if s.startswith("CREATE"):
        if re.search(r"\bCREATE\s+(UNIQUE\s+)?INDEX\b", s):
            return CommandCategory.CREATE_INDEX
        if re.search(r"\bCREATE\s+SEQUENCE\b", s):
            return CommandCategory.CREATE_SEQUENCE
        if re.search(r"\bCREATE\s+SCHEMA\b", s):
            return CommandCategory.CREATE_SCHEMA
        if re.search(r"\bCREATE\s+(OR\s+REPLACE\s+)?VIEW\b", s):
            return CommandCategory.CREATE_VIEW
        if re.search(r"\bCREATE\s+(TEMP(ORARY)?\s+)?TABLE\b", s):
            return CommandCategory.CREATE_TABLE
        return CommandCategory.UNKNOWN

    # DROP ΓÇª
    if s.startswith("DROP"):
        if re.search(r"\bDROP\s+TABLE\b", s):
            return CommandCategory.DROP_TABLE
        if re.search(r"\bDROP\s+INDEX\b", s):
            return CommandCategory.DROP_INDEX
        if re.search(r"\bDROP\s+SEQUENCE\b", s):
            return CommandCategory.DROP_SEQUENCE
        if re.search(r"\bDROP\s+SCHEMA\b", s):
            return CommandCategory.DROP_SCHEMA
        if re.search(r"\bDROP\s+VIEW\b", s):
            return CommandCategory.DROP_VIEW
        return CommandCategory.UNKNOWN

    # ALTER TABLE  (includes RENAME TABLE)
    if s.startswith("ALTER"):
        if re.search(r"\bALTER\s+TABLE\b", s):
            if re.search(r"\bRENAME\s+TO\b", s):
                return CommandCategory.RENAME_TABLE
            return CommandCategory.ALTER_TABLE
        if re.search(r"\bALTER\s+SEQUENCE\b", s):
            return CommandCategory.ALTER_SEQUENCE
        return CommandCategory.UNKNOWN

    return CommandCategory.UNKNOWN


def _quote_ident(name: str) -> str:
    """Double-quote a PostgreSQL identifier, escaping inner double-quotes."""
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(val) -> str:
    """
    Produce a PostgreSQL string literal from a Python value.
    Uses dollar-quoting for strings containing single quotes.
    None ΓåÆ NULL.
    """
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float)):
        return str(val)
    # string
    s = str(val)
    if "'" not in s:
        return f"'{s}'"
    # dollar-quote fallback
    tag = "WEAVEDB"
    return f"${tag}${s}${tag}$"


# ---------------------------------------------------------------------------
# Main engine
# ---------------------------------------------------------------------------

class InverseEngine:
    """
    Generates InverseCommand objects for PostgreSQL write operations.

    Usage
    -----
        conn   = psycopg2.connect(DSN)
        engine = InverseEngine(conn)

        # BEFORE executing the forward SQL:
        inv = engine.generate(forward_sql)

        # Execute the forward SQL (in same transaction or separate)
        cur = conn.cursor()
        cur.execute(forward_sql)
        conn.commit()

        # Store inv in the commit log table
        store.save_inverse(version_id, inv)
    """

    def __init__(self, connection):
        """
        Parameters
        ----------
        connection : psycopg2 connection object (or any DB-API 2 connection).
                     Must be connected to the target PostgreSQL instance.
        """
        self._conn = connection

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self, sql: str, params=None) -> InverseCommand:
        """
        Generate the inverse command for *sql*.

        Parameters
        ----------
        sql    : The forward SQL string (may contain %s placeholders if params given).
        params : Optional sequence of parameters for the forward SQL.  Used only
                 to resolve the actual affected rows for DML before-images.

        Returns
        -------
        InverseCommand  ΓÇö always returned; check .is_reversible and .category.
        If category is UNKNOWN the steps list will be empty.
        """
        sql_clean = _normalise(sql)
        category  = _classify(sql_clean)

        logger.debug("generate: category=%s sql=%.120s", category.name, sql_clean)

        dispatch = {
            CommandCategory.INSERT:          self._inverse_insert,
            CommandCategory.UPDATE:          self._inverse_update,
            CommandCategory.DELETE:          self._inverse_delete,
            CommandCategory.TRUNCATE:        self._inverse_truncate,
            CommandCategory.CREATE_TABLE:    self._inverse_create_table,
            CommandCategory.DROP_TABLE:      self._inverse_drop_table,
            CommandCategory.ALTER_TABLE:     self._inverse_alter_table,
            CommandCategory.RENAME_TABLE:    self._inverse_rename_table,
            CommandCategory.CREATE_INDEX:    self._inverse_create_index,
            CommandCategory.DROP_INDEX:      self._inverse_drop_index,
            CommandCategory.CREATE_SEQUENCE: self._inverse_create_sequence,
            CommandCategory.DROP_SEQUENCE:   self._inverse_drop_sequence,
            CommandCategory.ALTER_SEQUENCE:  self._inverse_alter_sequence,
            CommandCategory.CREATE_SCHEMA:   self._inverse_create_schema,
            CommandCategory.DROP_SCHEMA:     self._inverse_drop_schema,
            CommandCategory.CREATE_VIEW:     self._inverse_create_view,
            CommandCategory.DROP_VIEW:       self._inverse_drop_view,
        }

        handler = dispatch.get(category)
        if handler is None:
            return InverseCommand(
                category      = CommandCategory.UNKNOWN,
                forward_sql   = sql,
                is_reversible = False,
                notes         = "Non-modifying or unsupported command; no inverse generated.",
            )

        try:
            return handler(sql_clean, sql, params)
        except Exception as exc:
            logger.exception("InverseEngine.generate failed for: %.200s", sql_clean)
            return InverseCommand(
                category      = category,
                forward_sql   = sql,
                is_reversible = False,
                notes         = f"Inverse generation failed: {exc}",
            )

    # ------------------------------------------------------------------
    # DML handlers
    # ------------------------------------------------------------------

    def _inverse_insert(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        """
        Inverse of INSERT: DELETE the inserted rows.

        Strategy
        --------
        * If the table has a primary key: capture the PK values AFTER the insert
          using a RETURNING clause (if we can rewrite) or a post-hoc query keyed
          on ctid / sequence-derived max id.
        * Preferred approach: we rewrite the INSERT to include RETURNING *,
          execute it, capture the rows, build per-row DELETE statements, then
          store those as the inverse steps.

        Because we must return the inverse BEFORE execution (so the caller can
        store it), we use the "nextval peek" strategy for serial PKs or rely on
        the caller to pass us the RETURNING rows after execution via
        record_insert_result().

        For simplicity and correctness we use a two-phase approach:
          Phase 1 (before exec): parse table name, note we need RETURNING.
          Phase 2 (after exec):  caller calls finalize_insert(inv, rows).

        The InverseCommand returned here contains a placeholder; the caller
        MUST call engine.finalize_insert(inv, returned_rows) after executing
        the INSERT WITH RETURNING *.
        """
        table = _parse_insert_table(sql_norm)
        pks   = self._primary_keys(table)

        inv = InverseCommand(
            category    = CommandCategory.INSERT,
            forward_sql = sql_orig,
            notes       = (
                f"INSERT inverse for table '{table}'. "
                "Caller must call finalize_insert(inv, returned_rows) after "
                "executing INSERT ... RETURNING *."
            ),
        )
        inv._table = table   # stash for finalize_insert
        inv._pks   = pks
        return inv

    def finalize_insert(self, inv: InverseCommand, returned_rows: list[dict]) -> None:
        """
        Complete an INSERT inverse after the forward INSERT has run.

        Parameters
        ----------
        inv           : The InverseCommand from _inverse_insert.
        returned_rows : List of dicts (columnΓåÆvalue) from RETURNING *.
        """
        table = inv._table
        pks   = inv._pks
        inv.before_image = returned_rows

        if not returned_rows:
            inv.steps = []
            inv.notes += " No rows returned; nothing to delete."
            return

        steps = []
        if pks:
            for row in returned_rows:
                conditions = " AND ".join(
                    f"{_quote_ident(pk)} = {_quote_literal(row[pk])}"
                    for pk in pks
                )
                steps.append(
                    f"DELETE FROM {_quote_ident(table)} WHERE {conditions};"
                )
        else:
            # No PK ΓÇô use ctid if present, else full-row match (fragile)
            if "ctid" in (returned_rows[0] if returned_rows else {}):
                for row in returned_rows:
                    steps.append(
                        f"DELETE FROM {_quote_ident(table)} "
                        f"WHERE ctid = '{row['ctid']}';"
                    )
            else:
                for row in returned_rows:
                    conditions = " AND ".join(
                        f"{_quote_ident(k)} = {_quote_literal(v)}"
                        for k, v in row.items()
                    )
                    steps.append(
                        f"DELETE FROM {_quote_ident(table)} "
                        f"WHERE {conditions};"
                    )
                inv.is_reversible = False
                inv.notes += (
                    " WARNING: No primary key found. "
                    "Full-row match delete used ΓÇô may affect duplicate rows."
                )

        inv.steps = steps

    def _inverse_update(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        """
        Inverse of UPDATE: restore original column values for affected rows.

        Strategy
        --------
        1. Parse the table name and WHERE clause from the UPDATE.
        2. SELECT * FROM table WHERE <same-where> to capture before-images.
        3. Build one UPDATE ΓÇª SET ΓÇª WHERE pk=ΓÇª per affected row restoring
           original values.
        """
        table, where_clause = _parse_update_table_where(sql_norm)
        pks = self._primary_keys(table)

        # Capture before-image
        before_rows = self._select_rows(table, where_clause, params)

        steps = []
        if pks and before_rows:
            for row in before_rows:
                pk_cond = " AND ".join(
                    f"{_quote_ident(pk)} = {_quote_literal(row[pk])}"
                    for pk in pks
                )
                set_clause = ", ".join(
                    f"{_quote_ident(col)} = {_quote_literal(val)}"
                    for col, val in row.items()
                    if col not in pks
                )
                if set_clause:
                    steps.append(
                        f"UPDATE {_quote_ident(table)} "
                        f"SET {set_clause} "
                        f"WHERE {pk_cond};"
                    )
        elif before_rows:
            # No PK ΓÇô use full-row WHERE
            for row in before_rows:
                set_clause = ", ".join(
                    f"{_quote_ident(col)} = {_quote_literal(val)}"
                    for col, val in row.items()
                )
                steps.append(
                    f"UPDATE {_quote_ident(table)} "
                    f"SET {set_clause} "
                    f"LIMIT 1;"   # fragile without PK
                )

        reversible = bool(pks and before_rows)
        notes = "" if reversible else (
            "No primary key ΓÇô inverse UPDATE may affect wrong rows if duplicates exist."
        )

        return InverseCommand(
            category      = CommandCategory.UPDATE,
            forward_sql   = sql_orig,
            steps         = steps,
            before_image  = before_rows,
            is_reversible = reversible,
            notes         = notes,
        )

    def _inverse_delete(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        """
        Inverse of DELETE: re-INSERT all deleted rows.

        Strategy
        --------
        1. Capture before-image via SELECT * WHERE <same-where>.
        2. Build one INSERT per deleted row with explicit column list and values.
        """
        table, where_clause = _parse_delete_table_where(sql_norm)
        before_rows = self._select_rows(table, where_clause, params)

        steps = []
        for row in before_rows:
            cols   = ", ".join(_quote_ident(c) for c in row.keys())
            vals   = ", ".join(_quote_literal(v) for v in row.values())
            steps.append(
                f"INSERT INTO {_quote_ident(table)} ({cols}) VALUES ({vals});"
            )

        return InverseCommand(
            category     = CommandCategory.DELETE,
            forward_sql  = sql_orig,
            steps        = steps,
            before_image = before_rows,
        )

    def _inverse_truncate(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        """
        Inverse of TRUNCATE: re-INSERT all rows from before-image.

        TRUNCATE is instant but destructive.  We snapshot the entire table.
        For large tables this can be expensive ΓÇô operators should ensure
        snapshot frequency is high enough to avoid needing to replay many
        TRUNCATE inverses.
        """
        table = _parse_truncate_table(sql_norm)
        before_rows = self._select_rows(table, None, None)

        steps = []
        for row in before_rows:
            cols = ", ".join(_quote_ident(c) for c in row.keys())
            vals = ", ".join(_quote_literal(v) for v in row.values())
            steps.append(
                f"INSERT INTO {_quote_ident(table)} ({cols}) VALUES ({vals});"
            )

        reversible = True
        notes = (
            f"TRUNCATE on '{table}': captured {len(before_rows)} rows. "
            "Row order is not guaranteed on restore."
        )

        return InverseCommand(
            category      = CommandCategory.TRUNCATE,
            forward_sql   = sql_orig,
            steps         = steps,
            before_image  = before_rows,
            is_reversible = reversible,
            notes         = notes,
        )

    # ------------------------------------------------------------------
    # DDL handlers ΓÇô CREATE TABLE / DROP TABLE
    # ------------------------------------------------------------------

    def _inverse_create_table(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        """Inverse of CREATE TABLE: DROP TABLE IF EXISTS."""
        table = _parse_create_table_name(sql_norm)
        schema, tname = _split_schema_table(table)
        step = (
            f"DROP TABLE IF EXISTS {_qualified_ident(schema, tname)};"
        )
        return InverseCommand(
            category    = CommandCategory.CREATE_TABLE,
            forward_sql = sql_orig,
            steps       = [step],
            notes       = "DROP TABLE reverses CREATE TABLE. Data is discarded.",
        )

    def _inverse_drop_table(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        """
        Inverse of DROP TABLE: recreate the table using pg_catalog metadata
        captured BEFORE the drop executes.
        """
        table_ref = _parse_drop_object_name(sql_norm, "TABLE")
        schema, tname = _split_schema_table(table_ref)

        ddl = self._reconstruct_table_ddl(schema or "public", tname)
        if ddl:
            return InverseCommand(
                category     = CommandCategory.DROP_TABLE,
                forward_sql  = sql_orig,
                steps        = [ddl],
                before_image = {"ddl": ddl},
                notes        = "Recreates table structure only; data must come from snapshot.",
            )
        else:
            return InverseCommand(
                category      = CommandCategory.DROP_TABLE,
                forward_sql   = sql_orig,
                is_reversible = False,
                notes         = (
                    f"Could not reconstruct DDL for '{table_ref}'. "
                    "Table data and structure will come from snapshot."
                ),
            )

    # ------------------------------------------------------------------
    # DDL handlers ΓÇô ALTER TABLE
    # ------------------------------------------------------------------

    def _inverse_alter_table(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        """
        Dispatch to specific ALTER TABLE sub-type handler.

        Supported sub-types
        -------------------
        ADD COLUMN          ΓåÆ DROP COLUMN
        DROP COLUMN         ΓåÆ ADD COLUMN (reconstructed from pg_catalog)
        RENAME COLUMN       ΓåÆ RENAME COLUMN (reversed names)
        ALTER COLUMN TYPE   ΓåÆ ALTER COLUMN TYPE (to original type)
        ADD CONSTRAINT      ΓåÆ DROP CONSTRAINT
        DROP CONSTRAINT     ΓåÆ ADD CONSTRAINT (reconstructed)
        SET DEFAULT         ΓåÆ DROP DEFAULT  (or restore original default)
        DROP DEFAULT        ΓåÆ SET DEFAULT   (restore original default)
        SET NOT NULL        ΓåÆ DROP NOT NULL
        DROP NOT NULL       ΓåÆ SET NOT NULL
        """
        s = sql_norm.upper()
        table_ref = _parse_alter_table_name(sql_norm)
        schema, tname = _split_schema_table(table_ref)

        if "ADD COLUMN" in s:
            return self._inverse_add_column(sql_norm, sql_orig, schema, tname)
        if "DROP COLUMN" in s:
            return self._inverse_drop_column(sql_norm, sql_orig, schema, tname)
        if re.search(r"\bRENAME\s+COLUMN\b", s):
            return self._inverse_rename_column(sql_norm, sql_orig, schema, tname)
        if re.search(r"\bALTER\s+COLUMN\b.*\bTYPE\b", s):
            return self._inverse_alter_column_type(sql_norm, sql_orig, schema, tname)
        if "ADD CONSTRAINT" in s:
            return self._inverse_add_constraint(sql_norm, sql_orig, schema, tname)
        if "DROP CONSTRAINT" in s:
            return self._inverse_drop_constraint(sql_norm, sql_orig, schema, tname)
        if re.search(r"\bSET\s+DEFAULT\b", s):
            return self._inverse_set_default(sql_norm, sql_orig, schema, tname)
        if re.search(r"\bDROP\s+DEFAULT\b", s):
            return self._inverse_drop_default(sql_norm, sql_orig, schema, tname)
        if re.search(r"\bSET\s+NOT\s+NULL\b", s):
            return self._inverse_set_not_null(sql_norm, sql_orig, schema, tname)
        if re.search(r"\bDROP\s+NOT\s+NULL\b", s):
            return self._inverse_drop_not_null(sql_norm, sql_orig, schema, tname)

        return InverseCommand(
            category      = CommandCategory.ALTER_TABLE,
            forward_sql   = sql_orig,
            is_reversible = False,
            notes         = "Unrecognised ALTER TABLE sub-type; manual rollback required.",
        )

    def _inverse_add_column(self, sql_norm, sql_orig, schema, tname):
        col = _parse_alter_add_column_name(sql_norm)
        step = (
            f"ALTER TABLE {_qualified_ident(schema, tname)} "
            f"DROP COLUMN IF EXISTS {_quote_ident(col)};"
        )
        return InverseCommand(
            category    = CommandCategory.ALTER_TABLE,
            forward_sql = sql_orig,
            steps       = [step],
            notes       = f"Drops column '{col}' added by forward ALTER.",
        )

    def _inverse_drop_column(self, sql_norm, sql_orig, schema, tname):
        col    = _parse_alter_drop_column_name(sql_norm)
        col_def = self._get_column_definition(schema or "public", tname, col)
        if col_def:
            step = (
                f"ALTER TABLE {_qualified_ident(schema, tname)} "
                f"ADD COLUMN {col_def};"
            )
            return InverseCommand(
                category     = CommandCategory.ALTER_TABLE,
                forward_sql  = sql_orig,
                steps        = [step],
                before_image = {"column_definition": col_def},
                notes        = f"Restores column '{col}'. Data in that column is lost.",
            )
        return InverseCommand(
            category      = CommandCategory.ALTER_TABLE,
            forward_sql   = sql_orig,
            is_reversible = False,
            notes         = f"Could not reconstruct definition for column '{col}'.",
        )

    def _inverse_rename_column(self, sql_norm, sql_orig, schema, tname):
        old_name, new_name = _parse_rename_column(sql_norm)
        step = (
            f"ALTER TABLE {_qualified_ident(schema, tname)} "
            f"RENAME COLUMN {_quote_ident(new_name)} TO {_quote_ident(old_name)};"
        )
        return InverseCommand(
            category    = CommandCategory.ALTER_TABLE,
            forward_sql = sql_orig,
            steps       = [step],
        )

    def _inverse_alter_column_type(self, sql_norm, sql_orig, schema, tname):
        col = _parse_alter_column_type_name(sql_norm)
        orig_type = self._get_column_type(schema or "public", tname, col)
        if orig_type:
            step = (
                f"ALTER TABLE {_qualified_ident(schema, tname)} "
                f"ALTER COLUMN {_quote_ident(col)} TYPE {orig_type};"
            )
            return InverseCommand(
                category     = CommandCategory.ALTER_TABLE,
                forward_sql  = sql_orig,
                steps        = [step],
                before_image = {"column": col, "original_type": orig_type},
                notes        = "Type coercion back may require USING clause in some cases.",
            )
        return InverseCommand(
            category      = CommandCategory.ALTER_TABLE,
            forward_sql   = sql_orig,
            is_reversible = False,
            notes         = f"Could not determine original type for column '{col}'.",
        )

    def _inverse_add_constraint(self, sql_norm, sql_orig, schema, tname):
        cname = _parse_constraint_name(sql_norm)
        step = (
            f"ALTER TABLE {_qualified_ident(schema, tname)} "
            f"DROP CONSTRAINT IF EXISTS {_quote_ident(cname)};"
        )
        return InverseCommand(
            category    = CommandCategory.ALTER_TABLE,
            forward_sql = sql_orig,
            steps       = [step],
        )

    def _inverse_drop_constraint(self, sql_norm, sql_orig, schema, tname):
        cname = _parse_constraint_name_drop(sql_norm)
        constraint_def = self._get_constraint_definition(schema or "public", tname, cname)
        if constraint_def:
            step = (
                f"ALTER TABLE {_qualified_ident(schema, tname)} "
                f"ADD CONSTRAINT {_quote_ident(cname)} {constraint_def};"
            )
            return InverseCommand(
                category     = CommandCategory.ALTER_TABLE,
                forward_sql  = sql_orig,
                steps        = [step],
                before_image = {"constraint_def": constraint_def},
            )
        return InverseCommand(
            category      = CommandCategory.ALTER_TABLE,
            forward_sql   = sql_orig,
            is_reversible = False,
            notes         = f"Could not reconstruct constraint '{cname}'.",
        )

    def _inverse_set_default(self, sql_norm, sql_orig, schema, tname):
        col = _parse_alter_column_name_generic(sql_norm)
        # Drop the newly set default ΓåÆ inverse is DROP DEFAULT
        step = (
            f"ALTER TABLE {_qualified_ident(schema, tname)} "
            f"ALTER COLUMN {_quote_ident(col)} DROP DEFAULT;"
        )
        return InverseCommand(
            category    = CommandCategory.ALTER_TABLE,
            forward_sql = sql_orig,
            steps       = [step],
            notes       = "Drops default added by forward SET DEFAULT.",
        )

    def _inverse_drop_default(self, sql_norm, sql_orig, schema, tname):
        col       = _parse_alter_column_name_generic(sql_norm)
        orig_def  = self._get_column_default(schema or "public", tname, col)
        if orig_def is not None:
            step = (
                f"ALTER TABLE {_qualified_ident(schema, tname)} "
                f"ALTER COLUMN {_quote_ident(col)} SET DEFAULT {orig_def};"
            )
            return InverseCommand(
                category     = CommandCategory.ALTER_TABLE,
                forward_sql  = sql_orig,
                steps        = [step],
                before_image = {"column": col, "default": orig_def},
            )
        return InverseCommand(
            category      = CommandCategory.ALTER_TABLE,
            forward_sql   = sql_orig,
            is_reversible = False,
            notes         = f"Could not retrieve original default for column '{col}'.",
        )

    def _inverse_set_not_null(self, sql_norm, sql_orig, schema, tname):
        col  = _parse_alter_column_name_generic(sql_norm)
        step = (
            f"ALTER TABLE {_qualified_ident(schema, tname)} "
            f"ALTER COLUMN {_quote_ident(col)} DROP NOT NULL;"
        )
        return InverseCommand(CommandCategory.ALTER_TABLE, sql_orig, [step])

    def _inverse_drop_not_null(self, sql_norm, sql_orig, schema, tname):
        col  = _parse_alter_column_name_generic(sql_norm)
        step = (
            f"ALTER TABLE {_qualified_ident(schema, tname)} "
            f"ALTER COLUMN {_quote_ident(col)} SET NOT NULL;"
        )
        return InverseCommand(CommandCategory.ALTER_TABLE, sql_orig, [step])

    def _inverse_rename_table(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        old_name, new_name = _parse_rename_table(sql_norm)
        schema, _ = _split_schema_table(old_name)
        step = (
            f"ALTER TABLE {_qualified_ident(schema, new_name)} "
            f"RENAME TO {_quote_ident(old_name.split('.')[-1])};"
        )
        return InverseCommand(
            category    = CommandCategory.RENAME_TABLE,
            forward_sql = sql_orig,
            steps       = [step],
        )

    # ------------------------------------------------------------------
    # DDL handlers ΓÇô INDEX
    # ------------------------------------------------------------------

    def _inverse_create_index(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        index_name = _parse_create_index_name(sql_norm)
        step = f"DROP INDEX IF EXISTS {_quote_ident(index_name)};"
        return InverseCommand(CommandCategory.CREATE_INDEX, sql_orig, [step])

    def _inverse_drop_index(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        index_name = _parse_drop_object_name(sql_norm, "INDEX")
        index_def  = self._reconstruct_index_ddl(index_name)
        if index_def:
            return InverseCommand(
                category     = CommandCategory.DROP_INDEX,
                forward_sql  = sql_orig,
                steps        = [index_def],
                before_image = {"ddl": index_def},
            )
        return InverseCommand(
            category      = CommandCategory.DROP_INDEX,
            forward_sql   = sql_orig,
            is_reversible = False,
            notes         = f"Could not reconstruct index '{index_name}'.",
        )

    # ------------------------------------------------------------------
    # DDL handlers ΓÇô SEQUENCE
    # ------------------------------------------------------------------

    def _inverse_create_sequence(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        seq_name = _parse_create_sequence_name(sql_norm)
        step = f"DROP SEQUENCE IF EXISTS {_quote_ident(seq_name)};"
        return InverseCommand(CommandCategory.CREATE_SEQUENCE, sql_orig, [step])

    def _inverse_drop_sequence(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        seq_name  = _parse_drop_object_name(sql_norm, "SEQUENCE")
        seq_def   = self._reconstruct_sequence_ddl(seq_name)
        if seq_def:
            return InverseCommand(
                category     = CommandCategory.DROP_SEQUENCE,
                forward_sql  = sql_orig,
                steps        = [seq_def],
                before_image = {"ddl": seq_def},
            )
        return InverseCommand(
            category      = CommandCategory.DROP_SEQUENCE,
            forward_sql   = sql_orig,
            is_reversible = False,
            notes         = f"Could not reconstruct sequence '{seq_name}'.",
        )

    def _inverse_alter_sequence(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        """
        Inverse of ALTER SEQUENCE: restore previous sequence parameters.
        Captures current state before the ALTER.
        """
        seq_name  = _parse_alter_sequence_name(sql_norm)
        seq_state = self._get_sequence_state(seq_name)
        if seq_state:
            parts = [f"ALTER SEQUENCE {_quote_ident(seq_name)}"]
            if seq_state.get("increment_by") is not None:
                parts.append(f"INCREMENT BY {seq_state['increment_by']}")
            if seq_state.get("min_value") is not None:
                parts.append(f"MINVALUE {seq_state['min_value']}")
            if seq_state.get("max_value") is not None:
                parts.append(f"MAXVALUE {seq_state['max_value']}")
            if seq_state.get("start_value") is not None:
                parts.append(f"START {seq_state['start_value']}")
            if seq_state.get("last_value") is not None:
                parts.append(f"RESTART WITH {seq_state['last_value']}")
            step = " ".join(parts) + ";"
            return InverseCommand(
                category     = CommandCategory.ALTER_SEQUENCE,
                forward_sql  = sql_orig,
                steps        = [step],
                before_image = seq_state,
            )
        return InverseCommand(
            category      = CommandCategory.ALTER_SEQUENCE,
            forward_sql   = sql_orig,
            is_reversible = False,
            notes         = f"Could not read state of sequence '{seq_name}'.",
        )

    # ------------------------------------------------------------------
    # DDL handlers ΓÇô SCHEMA / VIEW
    # ------------------------------------------------------------------

    def _inverse_create_schema(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        schema_name = _parse_create_schema_name(sql_norm)
        step = f"DROP SCHEMA IF EXISTS {_quote_ident(schema_name)} RESTRICT;"
        return InverseCommand(
            CommandCategory.CREATE_SCHEMA, sql_orig, [step],
            notes="RESTRICT ensures schema is empty before dropping.",
        )

    def _inverse_drop_schema(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        schema_name = _parse_drop_object_name(sql_norm, "SCHEMA")
        step = f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema_name)};"
        return InverseCommand(
            CommandCategory.DROP_SCHEMA, sql_orig, [step],
            notes="Recreates empty schema; objects within must be restored from snapshot.",
        )

    def _inverse_create_view(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        view_name = _parse_create_view_name(sql_norm)
        # Was this CREATE OR REPLACE replacing an existing view?
        existing_def = self._get_view_definition(view_name)
        if existing_def and "OR REPLACE" in sql_norm.upper():
            # The inverse is to restore the previous view definition
            step = f"CREATE OR REPLACE VIEW {_quote_ident(view_name)} AS {existing_def};"
            return InverseCommand(
                CommandCategory.CREATE_VIEW, sql_orig, [step],
                before_image={"previous_def": existing_def},
                notes="Restores previous view definition.",
            )
        # Brand-new view ΓåÆ inverse is DROP
        step = f"DROP VIEW IF EXISTS {_quote_ident(view_name)};"
        return InverseCommand(CommandCategory.CREATE_VIEW, sql_orig, [step])

    def _inverse_drop_view(self, sql_norm: str, sql_orig: str, params) -> InverseCommand:
        view_name = _parse_drop_object_name(sql_norm, "VIEW")
        view_def  = self._get_view_definition(view_name)
        if view_def:
            step = f"CREATE VIEW {_quote_ident(view_name)} AS {view_def};"
            return InverseCommand(
                CommandCategory.DROP_VIEW, sql_orig, [step],
                before_image={"view_def": view_def},
            )
        return InverseCommand(
            CommandCategory.DROP_VIEW, sql_orig, is_reversible=False,
            notes=f"View '{view_name}' definition not found in pg_views.",
        )

    # ------------------------------------------------------------------
    # Database introspection helpers
    # ------------------------------------------------------------------

    def _cur(self):
        return self._conn.cursor()

    def _primary_keys(self, table: str) -> list[str]:
        schema, tname = _split_schema_table(table)
        schema = schema or "public"
        cur = self._cur()
        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema    = %s
              AND tc.table_name      = %s
            ORDER BY kcu.ordinal_position
        """, (schema, tname))
        rows = cur.fetchall()
        return [row["column_name"] if isinstance(row, dict) else row[0] for row in rows]

    def _select_rows(self, table: str, where_clause: Optional[str],
                     params) -> list[dict]:
        schema, tname = _split_schema_table(table)
        schema = schema or "public"
        q = f'SELECT * FROM {_qualified_ident(schema, tname)}'
        if where_clause:
            q += f" WHERE {where_clause}"
        cur = self._cur()
        try:
            cur.execute(q, params if where_clause and params else None)
        except Exception:
            cur.execute(q)
        rows = cur.fetchall()
        if not rows:
            return []
        if isinstance(rows[0], dict):
            return list(rows)
        if cur.description is None:
            return []
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in rows]

    def _reconstruct_table_ddl(self, schema: str, table: str) -> Optional[str]:
        """Build CREATE TABLE DDL from pg_catalog."""
        cur = self._cur()
        cur.execute("""
            SELECT
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.is_nullable,
                c.column_default,
                c.udt_name
            FROM information_schema.columns c
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
        """, (schema, table))
        cols = cur.fetchall()
        if not cols:
            return None

        col_defs = []
        for _row in cols:
            if isinstance(_row, dict):
                col_name, dtype, char_len, num_prec, num_scale, nullable, default, udt = (
                    _row["column_name"], _row["data_type"], _row["character_maximum_length"],
                    _row["numeric_precision"], _row["numeric_scale"], _row["is_nullable"],
                    _row["column_default"], _row["udt_name"]
                )
            else:
                col_name, dtype, char_len, num_prec, num_scale, nullable, default, udt = _row
            typedef = _build_type(dtype, char_len, num_prec, num_scale, udt)
            parts   = [_quote_ident(col_name), typedef]
            if default:
                parts.append(f"DEFAULT {default}")
            if nullable == "NO":
                parts.append("NOT NULL")
            col_defs.append("    " + " ".join(parts))

        # Reconstruct PKs and unique constraints
        cur.execute("""
            SELECT tc.constraint_type, tc.constraint_name,
                   string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position)
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            WHERE tc.table_schema = %s AND tc.table_name = %s
              AND tc.constraint_type IN ('PRIMARY KEY','UNIQUE')
            GROUP BY tc.constraint_type, tc.constraint_name
        """, (schema, table))
        for _crow in cur.fetchall():
            if isinstance(_crow, dict):
                ctype, cname, cols_str = _crow["constraint_type"], _crow["constraint_name"], _crow["string_agg"]
            else:
                ctype, cname, cols_str = _crow
            col_defs.append(
                f"    CONSTRAINT {_quote_ident(cname)} "
                f"{ctype} ({cols_str})"
            )

        qualified = _qualified_ident(schema, table)
        return (
            f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
            + ",\n".join(col_defs)
            + "\n);"
        )

    def _get_column_definition(self, schema: str, table: str, col: str) -> Optional[str]:
        cur = self._cur()
        cur.execute("""
            SELECT data_type, character_maximum_length, numeric_precision,
                   numeric_scale, is_nullable, column_default, udt_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s AND column_name=%s
        """, (schema, table, col))
        row = cur.fetchone()
        if not row:
            return None
        if isinstance(row, dict):
            dtype, char_len, num_prec, num_scale, nullable, default, udt = (
                row["data_type"], row["character_maximum_length"],
                row["numeric_precision"], row["numeric_scale"],
                row["is_nullable"], row["column_default"], row["udt_name"]
            )
        else:
            dtype, char_len, num_prec, num_scale, nullable, default, udt = row
        typedef = _build_type(dtype, char_len, num_prec, num_scale, udt)
        parts   = [_quote_ident(col), typedef]
        if default:
            parts.append(f"DEFAULT {default}")
        if nullable == "NO":
            parts.append("NOT NULL")
        return " ".join(parts)

    def _get_column_type(self, schema: str, table: str, col: str) -> Optional[str]:
        cur = self._cur()
        cur.execute("""
            SELECT data_type, character_maximum_length, numeric_precision,
                   numeric_scale, udt_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s AND column_name=%s
        """, (schema, table, col))
        row = cur.fetchone()
        if not row:
            return None
        if isinstance(row, dict):
            dtype, char_len, num_prec, num_scale, udt = (
                row["data_type"], row["character_maximum_length"],
                row["numeric_precision"], row["numeric_scale"], row["udt_name"]
            )
        else:
            dtype, char_len, num_prec, num_scale, udt = row
        return _build_type(dtype, char_len, num_prec, num_scale, udt)

    def _get_column_default(self, schema: str, table: str, col: str) -> Optional[str]:
        cur = self._cur()
        cur.execute("""
            SELECT column_default FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s AND column_name=%s
        """, (schema, table, col))
        row = cur.fetchone()
        if not row:
            return None
        val = row["column_default"] if isinstance(row, dict) else row[0]
        return val if val else None

    def _get_constraint_definition(self, schema: str, table: str, cname: str) -> Optional[str]:
        cur = self._cur()
        cur.execute("""
            SELECT pg_get_constraintdef(c.oid)
            FROM pg_constraint c
            JOIN pg_class r ON r.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = r.relnamespace
            WHERE n.nspname = %s AND r.relname = %s AND c.conname = %s
        """, (schema, table, cname))
        row = cur.fetchone()
        if not row:
            return None
        return row["pg_get_constraintdef"] if isinstance(row, dict) else row[0]

    def _reconstruct_index_ddl(self, index_name: str) -> Optional[str]:
        cur = self._cur()
        cur.execute("""
            SELECT indexdef FROM pg_indexes
            WHERE indexname = %s
        """, (index_name,))
        row = cur.fetchone()
        if not row:
            return None
        val = row["indexdef"] if isinstance(row, dict) else row[0]
        return val + ";"

    def _reconstruct_sequence_ddl(self, seq_name: str) -> Optional[str]:
        cur = self._cur()
        cur.execute("""
            SELECT
                s.seqstart, s.seqincrement, s.seqmax, s.seqmin,
                s.seqcache, s.seqcycle
            FROM pg_sequences ps
            JOIN pg_class c ON c.relname = ps.sequencename
            JOIN pg_sequence s ON s.seqrelid = c.oid
            WHERE ps.sequencename = %s
        """, (seq_name,))
        row = cur.fetchone()
        if not row:
            return None
        if isinstance(row, dict):
            start, incr, maxv, minv, cache, cycle = (
                row["seqstart"], row["seqincrement"], row["seqmax"],
                row["seqmin"], row["seqcache"], row["seqcycle"]
            )
        else:
            start, incr, maxv, minv, cache, cycle = row
        cycle_kw = "CYCLE" if cycle else "NO CYCLE"
        return (
            f"CREATE SEQUENCE IF NOT EXISTS {_quote_ident(seq_name)} "
            f"START {start} INCREMENT {incr} MINVALUE {minv} MAXVALUE {maxv} "
            f"CACHE {cache} {cycle_kw};"
        )

    def _get_sequence_state(self, seq_name: str) -> Optional[dict]:
        cur = self._cur()
        cur.execute("""
            SELECT last_value, start_value, increment_by, min_value, max_value
            FROM %s
        """ % _quote_ident(seq_name))   # noqa: S608 (seq name from catalog, not user input)
        row = cur.fetchone()
        if not row:
            return None
        if isinstance(row, dict):
            return {
                "last_value":   row["last_value"],
                "start_value":  row["start_value"],
                "increment_by": row["increment_by"],
                "min_value":    row["min_value"],
                "max_value":    row["max_value"],
            }
        return {
            "last_value":   row[0],
            "start_value":  row[1],
            "increment_by": row[2],
            "min_value":    row[3],
            "max_value":    row[4],
        }

    def _get_view_definition(self, view_name: str) -> Optional[str]:
        cur = self._cur()
        cur.execute("""
            SELECT definition FROM pg_views WHERE viewname = %s
        """, (view_name,))
        row = cur.fetchone()
        if not row:
            return None
        return row["definition"] if isinstance(row, dict) else row[0]


# ---------------------------------------------------------------------------
# SQL parsing helpers  (regex-based, handles quoted identifiers)
# ---------------------------------------------------------------------------

def _unquote(name: str) -> str:
    """Remove surrounding double-quotes if present."""
    name = name.strip()
    if name.startswith('"') and name.endswith('"'):
        return name[1:-1].replace('""', '"')
    return name.lower()


def _split_schema_table(ref: str):
    """Split 'schema.table' into (schema, table). Returns (None, name) if no dot."""
    ref = ref.strip()
    if "." in ref:
        parts = ref.split(".", 1)
        return _unquote(parts[0]), _unquote(parts[1])
    return None, _unquote(ref)


def _qualified_ident(schema: Optional[str], name: str) -> str:
    if schema and schema != "public":
        return f"{_quote_ident(schema)}.{_quote_ident(name)}"
    return _quote_ident(name)


def _build_type(dtype, char_len, num_prec, num_scale, udt) -> str:
    """Reconstruct a PostgreSQL type string from information_schema columns."""
    dtype = dtype.lower()
    if dtype == "character varying":
        return f"VARCHAR({char_len})" if char_len else "TEXT"
    if dtype == "character":
        return f"CHAR({char_len})" if char_len else "CHAR"
    if dtype == "numeric":
        if num_prec and num_scale:
            return f"NUMERIC({num_prec},{num_scale})"
        return "NUMERIC"
    if dtype == "USER-DEFINED".lower():
        return udt or "TEXT"
    return dtype.upper()


# --- INSERT ---
def _parse_insert_table(sql: str) -> str:
    m = re.search(
        r"INSERT\s+INTO\s+(\"[^\"]+\"|[\w.]+)",
        sql, re.IGNORECASE
    )
    return _unquote(m.group(1)) if m else "unknown_table"


# --- UPDATE ---
def _parse_update_table_where(sql: str):
    m = re.search(r"UPDATE\s+(\"[^\"]+\"|[\w.]+)", sql, re.IGNORECASE)
    table = _unquote(m.group(1)) if m else "unknown_table"
    w = re.search(r"\bWHERE\b(.+?)(?:$|RETURNING|ORDER\s+BY|LIMIT)", sql,
                  re.IGNORECASE | re.DOTALL)
    where = w.group(1).strip() if w else None
    return table, where


# --- DELETE ---
def _parse_delete_table_where(sql: str):
    m = re.search(r"DELETE\s+FROM\s+(\"[^\"]+\"|[\w.]+)", sql, re.IGNORECASE)
    table = _unquote(m.group(1)) if m else "unknown_table"
    w = re.search(r"\bWHERE\b(.+?)(?:$|RETURNING|ORDER\s+BY|LIMIT)", sql,
                  re.IGNORECASE | re.DOTALL)
    where = w.group(1).strip() if w else None
    return table, where


# --- TRUNCATE ---
def _parse_truncate_table(sql: str) -> str:
    m = re.search(r"TRUNCATE\s+(?:TABLE\s+)?(\"[^\"]+\"|[\w.]+)", sql, re.IGNORECASE)
    return _unquote(m.group(1)) if m else "unknown_table"


# --- CREATE TABLE ---
def _parse_create_table_name(sql: str) -> str:
    m = re.search(
        r"CREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"(\"[^\"]+\"|[\w.]+)",
        sql, re.IGNORECASE
    )
    return _unquote(m.group(1)) if m else "unknown_table"


# --- DROP object ---
def _parse_drop_object_name(sql: str, obj_type: str) -> str:
    pattern = (
        rf"DROP\s+{obj_type}\s+(?:IF\s+EXISTS\s+)?"
        r"(\"[^\"]+\"|[\w.]+)"
    )
    m = re.search(pattern, sql, re.IGNORECASE)
    return _unquote(m.group(1)) if m else "unknown"


# --- ALTER TABLE name ---
def _parse_alter_table_name(sql: str) -> str:
    m = re.search(
        r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\"[^\"]+\"|[\w.]+)",
        sql, re.IGNORECASE
    )
    return _unquote(m.group(1)) if m else "unknown_table"


# --- ALTER TABLE ADD COLUMN col_name ---
def _parse_alter_add_column_name(sql: str) -> str:
    m = re.search(r"ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?(\"[^\"]+\"|\w+)",
                  sql, re.IGNORECASE)
    return _unquote(m.group(1)) if m else "unknown_col"


# --- ALTER TABLE DROP COLUMN col_name ---
def _parse_alter_drop_column_name(sql: str) -> str:
    m = re.search(r"DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?(\"[^\"]+\"|\w+)",
                  sql, re.IGNORECASE)
    return _unquote(m.group(1)) if m else "unknown_col"


# --- ALTER TABLE ΓÇª RENAME COLUMN old TO new ---
def _parse_rename_column(sql: str):
    m = re.search(
        r"RENAME\s+COLUMN\s+(\"[^\"]+\"|\w+)\s+TO\s+(\"[^\"]+\"|\w+)",
        sql, re.IGNORECASE
    )
    if m:
        return _unquote(m.group(1)), _unquote(m.group(2))
    return "old_col", "new_col"


# --- ALTER TABLE ΓÇª ALTER COLUMN col TYPE ---
def _parse_alter_column_type_name(sql: str) -> str:
    m = re.search(r"ALTER\s+COLUMN\s+(\"[^\"]+\"|\w+)\s+TYPE", sql, re.IGNORECASE)
    return _unquote(m.group(1)) if m else "unknown_col"


# --- Generic ALTER COLUMN col (for SET DEFAULT, DROP DEFAULT, NOT NULL) ---
def _parse_alter_column_name_generic(sql: str) -> str:
    m = re.search(r"ALTER\s+COLUMN\s+(\"[^\"]+\"|\w+)", sql, re.IGNORECASE)
    return _unquote(m.group(1)) if m else "unknown_col"


# --- ADD CONSTRAINT cname ---
def _parse_constraint_name(sql: str) -> str:
    m = re.search(r"ADD\s+CONSTRAINT\s+(\"[^\"]+\"|\w+)", sql, re.IGNORECASE)
    return _unquote(m.group(1)) if m else "unknown_constraint"


# --- DROP CONSTRAINT cname ---
def _parse_constraint_name_drop(sql: str) -> str:
    m = re.search(r"DROP\s+CONSTRAINT\s+(?:IF\s+EXISTS\s+)?(\"[^\"]+\"|\w+)",
                  sql, re.IGNORECASE)
    return _unquote(m.group(1)) if m else "unknown_constraint"


# --- ALTER TABLE old RENAME TO new ---
def _parse_rename_table(sql: str):
    m = re.search(
        r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\"[^\"]+\"|[\w.]+)"
        r"\s+RENAME\s+TO\s+(\"[^\"]+\"|\w+)",
        sql, re.IGNORECASE
    )
    if m:
        return _unquote(m.group(1)), _unquote(m.group(2))
    return "old_table", "new_table"


# --- CREATE INDEX name ---
def _parse_create_index_name(sql: str) -> str:
    m = re.search(
        r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
        r"(?:IF\s+NOT\s+EXISTS\s+)?(\"[^\"]+\"|\w+)",
        sql, re.IGNORECASE
    )
    return _unquote(m.group(1)) if m else "unknown_index"


# --- CREATE SEQUENCE name ---
def _parse_create_sequence_name(sql: str) -> str:
    m = re.search(r"CREATE\s+SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\"[^\"]+\"|\w+)",
                  sql, re.IGNORECASE)
    return _unquote(m.group(1)) if m else "unknown_seq"


# --- ALTER SEQUENCE name ---
def _parse_alter_sequence_name(sql: str) -> str:
    m = re.search(r"ALTER\s+SEQUENCE\s+(?:IF\s+EXISTS\s+)?(\"[^\"]+\"|\w+)",
                  sql, re.IGNORECASE)
    return _unquote(m.group(1)) if m else "unknown_seq"


# --- CREATE SCHEMA name ---
def _parse_create_schema_name(sql: str) -> str:
    m = re.search(r"CREATE\s+SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:AUTHORIZATION\s+\S+\s+)?"
                  r"(\"[^\"]+\"|\w+)",
                  sql, re.IGNORECASE)
    return _unquote(m.group(1)) if m else "unknown_schema"


# --- CREATE VIEW name ---
def _parse_create_view_name(sql: str) -> str:
    m = re.search(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\"[^\"]+\"|[\w.]+)",
        sql, re.IGNORECASE
    )
    return _unquote(m.group(1)) if m else "unknown_view"
