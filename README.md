# Relational SQL Database Workbench

Version Control for Databases - A web-based workbench with built-in version control capabilities for relational SQL databases.

## Project Overview

This system provides a web-based workbench for relational SQL databases that allows users to run queries through a UI and view outputs, while also providing version control capabilities for database write operations. The system enables rollback to previous versions using a combination of command history, inverse operations, and mandatory snapshots.

### Key Features

- **SQL Query Execution**: Execute SQL queries via web UI with immediate results display
- **Change Tracking**: Track all database-modifying SQL commands (INSERT, UPDATE, DELETE, CREATE, ALTER, DROP)
- **Version History**: Maintain a versioned commit history of all database modifications
- **Rollback Mechanism**: Roll back database state to any previous version
- **Snapshot System**: Optimized rollback using mandatory snapshots at configurable intervals
- **Schema Support**: Support for schema-altering commands (DDL operations)

## Architecture

### Major Components

1. **Web-based User Interface**
   - SQL command editor
   - Query results display panel
   - Commit history timeline viewer
   - Rollback workflow interface
   - Snapshot configuration screen

2. **Backend Processing Service**
   - Query validation and execution
   - Version management
   - Inverse operation generation

3. **Event Streaming Layer**
   - Command serialization
   - Concurrency handling through event ordering

4. **Snapshot Storage System**
   - Cloud-based object storage for snapshots
   - Periodic snapshot creation

## Technology Stack

- **Database**: PostgreSQL (SQL-based relational database)
- **Backend**: Python (FastAPI + Django ORM)
- **Frontend**: Next.js 16 (React 19), TypeScript
- **Styling**: Tailwind CSS 4
- **Code Editor**: CodeMirror 6 (SQL language support, autocomplete, one-dark theme)
- **Terminal**: xterm.js 6 over **WebSocket** (real-time interactive psql sessions)
- **Layout**: react-resizable-panels
- **Storage**: AWS S3 (for database snapshots)

## Frontend ↔ Backend Connection

### REST API

All HTTP communication goes through a centralized `request()` helper in `frontend/src/lib/api.ts`.

- **Base URL**: Configured via `NEXT_PUBLIC_API_URL` env variable (defaults to `http://localhost:8001`)
- **Authentication**: JWT Bearer token auto-injected from `localStorage` on every request; tokens are cleared on `401` to force re-login
- **Auth lifecycle**: Managed in `frontend/src/lib/auth-context.tsx` (login, register, token refresh)
- **Workspace state**: `frontend/src/lib/workspace-context.tsx` holds the active connection and schema metadata

### WebSocket (Interactive Terminal)

The terminal uses a WebSocket connection for real-time bidirectional I/O:

1. Frontend calls `POST /terminal/ticket` to obtain a short-lived, single-use ticket (30s TTL) — this avoids exposing the JWT in WebSocket query strings
2. Frontend opens `ws://<host>/terminal/ws?ticket=<ticket>` using xterm.js (`frontend/src/components/workbench/xterm-terminal.tsx`)
3. Backend (`fastapi_backend/app/routes/terminal_routes.py`) validates the ticket, opens a `psycopg2` connection to the user's database, and streams terminal I/O over the WebSocket

## Core Functionality

### Version Control Mechanism

The system implements version control for databases through:

1. **Commits**: Every successful database-modifying command is treated as a distinct commit
2. **Snapshots**: Mandatory database state snapshots created at user-configurable intervals
3. **Inverse Operations (Anti-commands)**: For each write operation, an inverse operation is stored to enable rollback
4. **Event Ordering**: All operations are strictly ordered through an event streaming mechanism

### Rollback Process

When rolling back to a previous version:
1. System identifies the nearest snapshot before the target version
2. Restores the database from that snapshot
3. Replays inverse operations between the snapshot and target version
4. If inverse operation fails, impact is limited to the interval between adjacent snapshots

### Snapshot Configuration

- **Default frequency**: One snapshot per five commits
- **Maximum frequency**: One snapshot per commit
- **Minimum frequency**: One snapshot at system initialization
- **User-configurable**: Frequency can be adjusted based on needs

## Supported SQL Operations

### Tracked Operations (Version Controlled)
- `INSERT` - Add new records
- `UPDATE` - Modify existing records
- `DELETE` - Remove records
- `CREATE` - Create database objects
- `ALTER` - Modify schema
- `DROP` - Remove database objects

### Non-Tracked Operations
- `SELECT` and other read-only queries can be executed but are not version controlled

## Getting Started

### Prerequisites

- Python 3.x
- PostgreSQL database
- Cloud storage access (for snapshots)

### Installation

```bash
# Clone the repository
git clone <repository-url>

# Install dependencies
pip install -r requirements.txt

# Configure database connection
# Edit config.yaml with your database credentials
```

### Configuration

Edit [config.yaml](config.yaml) to set:
- Database connection parameters
- Snapshot frequency
- Storage settings
- Event streaming configuration

## Usage

### Running Queries

1. Access the web UI
2. Enter SQL command in the editor
3. Execute to see results
4. View commit history for write operations

### Rolling Back

1. Navigate to commit history timeline
2. Select target version
3. Review rollback plan
4. Confirm and monitor progress
5. Verify restored state with SELECT queries

## Design Standards

### Naming Conventions

| Element | Style | Example |
|---------|-------|---------|
| Classes | PascalCase | `DatabaseAdapter`, `RollbackManager` |
| Methods | camelCase | `executeWrite()`, `loadSnapshot()` |
| Variables | camelCase | `versionId`, `sqlCommand` |
| Constants | UPPER_CASE | `DEFAULT_SNAPSHOT_FREQ` |
| Database Tables | lower_case | `commit_events`, `snapshots` |

### Code Comments

- Use `TODO` and `FIXME` keywords for planned work and issues
- Include author name/identifier for non-trivial comments

## System Requirements

### Operating Environment

- SQL-based relational database
- Backend service environment
- Web browser (for UI access)
- Networked deployment environment

### Design Constraints

- Only SQL-based databases supported
- Only data-modifying queries are tracked
- Snapshots are mandatory
- Event streaming required for concurrency

### Multi-Table Support (Future Version)
- Track dependencies between tables
- Account for foreign key relationships in anti-commands
- Coordinated rollback across related tables

## Documentation

- **SRS Document**: See [docs/srs.pdf](docs/srs.pdf) for complete software requirements specification
- **User Guide**: Instructions for SQL query submission, output interpretation, and rollback operations
- **Admin Guide**: System maintenance, availability, and storage infrastructure documentation
- **Inline Help**: Version control terminology and configuration guidance

## Project Scope

**Current Version**: Single table tracking within a database
**Future Versions**: Will expand to support tracking multiple tables within a database

## Glossary

- **Version**: Logical state of the database after applying a set of commands
- **Commit**: A successful database-modifying operation that advances the version state
- **Snapshot**: Stored representation of complete database state at a specific version
- **Anti-command**: Inverse operation used to reverse a commit during rollback
- **Event Stream**: Ordered sequence of database modifications ensuring consistency

## License

[License information to be added]

---

For detailed requirements and system specifications, please refer to the [Software Requirements Specification](docs/srs.pdf).
