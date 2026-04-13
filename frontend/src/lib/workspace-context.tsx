import { useState, useCallback, useEffect, useRef, createContext, useContext, type ReactNode } from "react";
import {
  listConnections,
  createConnection as apiCreateConnection,
  deleteConnection as apiDeleteConnection,
  executeSQL,
  createCommit as apiCreateCommit,
  listCommits,
  listSnapshots,
  createSnapshot as apiCreateSnapshot,
  getSnapshotFrequency,
  setSnapshotFrequency as apiSetSnapshotFreq,
  rollbackToVersion as apiRollback,
  type ConnectionProfile,
  type QueryResult,
  type Commit,
  type Snapshot,
  type CreateConnectionRequest,
  type RollbackResult,
} from "@/lib/api";

const _WRITE_FIRST_TOKENS = new Set(["INSERT","UPDATE","DELETE","CREATE","DROP","ALTER","TRUNCATE","RENAME"]);

function isWriteSQL(sql: string): boolean {
  const first = sql.trim().split(/\s+/)[0]?.toUpperCase();
  return _WRITE_FIRST_TOKENS.has(first);
}

export type { ConnectionProfile, QueryResult, Commit, Snapshot };

export interface SchemaTree {
  name: string;
  tables: { name: string; columns: { name: string; type: string; isPk: boolean }[] }[];
  views: { name: string }[];
  functions: { name: string }[];
  sequences: { name: string }[];
}

interface WorkspaceContextType {
  connections: ConnectionProfile[];
  activeConnection: ConnectionProfile | null;
  setActiveConnection: (conn: ConnectionProfile | null) => void;
  refreshConnections: () => Promise<void>;
  addConnection: (data: CreateConnectionRequest) => Promise<ConnectionProfile>;
  removeConnection: (id: number) => Promise<void>;
  schemas: SchemaTree[];
  commits: Commit[];
  snapshots: Snapshot[];
  refreshCommits: () => Promise<void>;
  refreshSnapshots: () => Promise<void>;
  executeQuery: (sql: string) => Promise<QueryResult>;
  queryResult: QueryResult | null;
  rollback: (targetVersionId: string) => Promise<RollbackResult>;
  createSnapshot: () => Promise<Snapshot>;
  snapshotFrequency: number;
  setSnapshotFrequency: (freq: number) => Promise<void>;
  terminalHistory: string[];
  addTerminalLine: (line: string) => void;
  isLoadingConnections: boolean;
}

const WorkspaceContext = createContext<WorkspaceContextType | null>(null);

export function useWorkspace() {
  const ctx = useContext(WorkspaceContext);
  if (!ctx) throw new Error("useWorkspace must be used within WorkspaceProvider");
  return ctx;
}

export function WorkspaceProvider({ children }: { children: ReactNode }) {
  const [connections, setConnections] = useState<ConnectionProfile[]>([]);
  const [activeConnection, setActiveConnection] = useState<ConnectionProfile | null>(null);
  const [isLoadingConnections, setIsLoadingConnections] = useState(false);
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);
  const [schemas, setSchemas] = useState<SchemaTree[]>([]);
  const [commits, setCommits] = useState<Commit[]>([]);
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [snapshotFrequency, setSnapshotFreqState] = useState(5);
  const [terminalHistory, setTerminalHistory] = useState<string[]>([]);

  const activeConnectionRef = useRef(activeConnection);
  activeConnectionRef.current = activeConnection;

  // ---------- Connections ----------
  const refreshConnections = useCallback(async () => {
    setIsLoadingConnections(true);
    try {
      const data = await listConnections();
      setConnections(data);
      if (activeConnectionRef.current && !data.find((c) => c.id === activeConnectionRef.current!.id)) {
        setActiveConnection(null);
      }
    } finally {
      setIsLoadingConnections(false);
    }
  }, []);

  const addConnection = useCallback(async (data: CreateConnectionRequest) => {
    const conn = await apiCreateConnection(data);
    setConnections((prev) => [...prev, conn]);
    return conn;
  }, []);

  const removeConnection = useCallback(async (id: number) => {
    await apiDeleteConnection(id);
    setConnections((prev) => prev.filter((c) => c.id !== id));
    if (activeConnectionRef.current?.id === id) setActiveConnection(null);
  }, []);

  // Load connections on mount
  useEffect(() => {
    refreshConnections();
  }, [refreshConnections]);

  // ---------- Schema metadata ----------
  const refreshSchemas = useCallback(async (connId: number) => {
    try {
      const result = await executeSQL(connId,
        `SELECT
           n.nspname AS schema_name,
           c.relname AS object_name,
           CASE c.relkind
             WHEN 'r' THEN 'table'
             WHEN 'v' THEN 'view'
             WHEN 'S' THEN 'sequence'
           END AS object_type,
           a.attname AS column_name,
           pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
           COALESCE(
             (SELECT true FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary AND a.attnum = ANY(i.indkey)),
             false
           ) AS is_pk
         FROM pg_namespace n
         JOIN pg_class c ON c.relnamespace = n.oid AND c.relkind IN ('r','v','S')
         LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
         WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
         ORDER BY n.nspname, c.relkind, c.relname, a.attnum`
      );

      const schemaMap = new Map<string, SchemaTree>();
      for (const row of result.rows) {
        const [sName, objName, objType, colName, colType, isPk] = row as [string, string, string, string | null, string | null, boolean];
        if (!schemaMap.has(sName)) {
          schemaMap.set(sName, { name: sName, tables: [], views: [], functions: [], sequences: [] });
        }
        const schema = schemaMap.get(sName)!;

        if (objType === "table") {
          let table = schema.tables.find((t) => t.name === objName);
          if (!table) { table = { name: objName, columns: [] }; schema.tables.push(table); }
          if (colName) table.columns.push({ name: colName, type: colType || "unknown", isPk: !!isPk });
        } else if (objType === "view") {
          if (!schema.views.find((v) => v.name === objName)) schema.views.push({ name: objName });
        } else if (objType === "sequence") {
          if (!schema.sequences.find((s) => s.name === objName)) schema.sequences.push({ name: objName });
        }
      }

      // Fetch functions
      try {
        const fnResult = await executeSQL(connId,
          `SELECT n.nspname, p.proname
           FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
           WHERE n.nspname NOT IN ('pg_catalog','information_schema')
           ORDER BY n.nspname, p.proname`
        );
        for (const row of fnResult.rows) {
          const [sName, fnName] = row as [string, string];
          if (!schemaMap.has(sName)) {
            schemaMap.set(sName, { name: sName, tables: [], views: [], functions: [], sequences: [] });
          }
          schemaMap.get(sName)!.functions.push({ name: fnName });
        }
      } catch { /* functions query may fail on limited permissions */ }

      setSchemas(Array.from(schemaMap.values()));
    } catch {
      setSchemas([]);
    }
  }, []);

  // ---------- Commits & Snapshots ----------
  const refreshCommits = useCallback(async () => {
    if (!activeConnectionRef.current) { setCommits([]); return; }
    try {
      const data = await listCommits(activeConnectionRef.current.id);
      setCommits(data);
    } catch { setCommits([]); }
  }, []);

  const refreshSnapshots = useCallback(async () => {
    if (!activeConnectionRef.current) { setSnapshots([]); return; }
    try {
      const data = await listSnapshots(activeConnectionRef.current.id);
      setSnapshots(data);
    } catch { setSnapshots([]); }
  }, []);

  // When active connection changes, fetch everything
  useEffect(() => {
    if (activeConnection) {
      refreshSchemas(activeConnection.id);
      refreshCommits();
      refreshSnapshots();
      getSnapshotFrequency(activeConnection.id).then(setSnapshotFreqState).catch(() => {});
      setTerminalHistory([
        "psql (16.2)",
        "Type \"help\" for help.",
        "",
        `${activeConnection.database_name}=> `,
      ]);
    } else {
      setSchemas([]);
      setCommits([]);
      setSnapshots([]);
      setTerminalHistory([]);
    }
  }, [activeConnection, refreshSchemas, refreshCommits, refreshSnapshots]);

  // ---------- Query ----------
  const runQuery = useCallback(async (sql: string): Promise<QueryResult> => {
    if (!activeConnectionRef.current) throw new Error("No active connection");
    const start = performance.now();

    if (isWriteSQL(sql)) {
      // Route writes through the commit pipeline (auto-generates inverse)
      const commit = await apiCreateCommit(activeConnectionRef.current.id, sql);
      const elapsed = Math.round(performance.now() - start);
      // Refresh commits in background
      refreshCommits();
      const synthetic: QueryResult = {
        columns: ["version_id", "seq", "status"],
        rows: [[commit.version_id, String(commit.seq), commit.status]],
        rowcount: 1,
        status: "success",
      };
      (synthetic as any).elapsed = elapsed;
      setQueryResult(synthetic);
      return synthetic;
    }

    const result = await executeSQL(activeConnectionRef.current.id, sql);
    (result as any).elapsed = Math.round(performance.now() - start);
    setQueryResult(result as any);
    return result as any;
  }, [refreshCommits]);

  // ---------- Rollback ----------
  const rollback = useCallback(async (targetVersionId: string) => {
    if (!activeConnectionRef.current) throw new Error("No active connection");
    const result = await apiRollback(activeConnectionRef.current.id, targetVersionId);
    await refreshCommits();
    return result;
  }, [refreshCommits]);

  // ---------- Snapshots ----------
  const doCreateSnapshot = useCallback(async () => {
    if (!activeConnectionRef.current) throw new Error("No active connection");
    const snap = await apiCreateSnapshot(activeConnectionRef.current.id);
    await refreshSnapshots();
    return snap;
  }, [refreshSnapshots]);

  const doSetSnapshotFrequency = useCallback(async (freq: number) => {
    if (!activeConnectionRef.current) throw new Error("No active connection");
    await apiSetSnapshotFreq(activeConnectionRef.current.id, freq);
    setSnapshotFreqState(freq);
  }, []);

  // ---------- Terminal ----------
  const addTerminalLine = useCallback((line: string) => {
    setTerminalHistory((prev) => [...prev, line]);
  }, []);

  return (
    <WorkspaceContext.Provider value={{
      connections,
      activeConnection,
      setActiveConnection,
      refreshConnections,
      addConnection,
      removeConnection,
      isLoadingConnections,
      schemas,
      commits,
      snapshots,
      refreshCommits,
      refreshSnapshots,
      executeQuery: runQuery,
      queryResult,
      rollback,
      createSnapshot: doCreateSnapshot,
      snapshotFrequency,
      setSnapshotFrequency: doSetSnapshotFrequency,
      terminalHistory,
      addTerminalLine,
    }}>
      {children}
    </WorkspaceContext.Provider>
  );
}
