const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8001";

interface RequestOptions {
  method?: string;
  body?: unknown;
  headers?: Record<string, string>;
}

export class ApiError extends Error {
  status: number;
  detail: unknown;
  constructor(status: number, detail: unknown) {
    super(typeof detail === "string" ? detail : JSON.stringify(detail));
    this.status = status;
    this.detail = detail;
  }
}

let _refreshing: Promise<TokenResponse> | null = null;

async function request<T>(path: string, opts: RequestOptions = {}): Promise<T> {
  const { method = "GET", body, headers = {} } = opts;

  const token = localStorage.getItem("access_token");
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  const fetchOpts: RequestInit = {
    method,
    headers: {
      "Content-Type": "application/json",
      ...headers,
    },
  };

  if (body !== undefined) {
    fetchOpts.body = JSON.stringify(body);
  }

  let res = await fetch(`${API_BASE}${path}`, fetchOpts);

  if (res.status === 204) return undefined as T;

  // On 401, try refreshing the token once (skip for auth endpoints)
  if (res.status === 401 && !path.startsWith("/auth/")) {
    const refresh = localStorage.getItem("refresh_token");
    if (refresh) {
      try {
        if (!_refreshing) {
          _refreshing = (async () => {
            const r = await fetch(`${API_BASE}/auth/token/refresh`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ refresh }),
            });
            if (!r.ok) throw new Error("refresh failed");
            return r.json();
          })();
        }
        const data: TokenResponse = await _refreshing;
        _refreshing = null;
        localStorage.setItem("access_token", data.access);
        if (data.refresh) localStorage.setItem("refresh_token", data.refresh);
        headers["Authorization"] = `Bearer ${data.access}`;
        res = await fetch(`${API_BASE}${path}`, { ...fetchOpts, headers: { ...fetchOpts.headers as Record<string, string>, Authorization: `Bearer ${data.access}` } });
      } catch {
        _refreshing = null;
        localStorage.removeItem("access_token");
        localStorage.removeItem("refresh_token");
      }
    } else {
      localStorage.removeItem("access_token");
    }
  }

  if (res.status === 204) return undefined as T;

  const json = await res.json().catch(() => null);

  if (!res.ok) {
    throw new ApiError(res.status, json?.detail || json || res.statusText);
  }

  return json as T;
}

// ---------- Auth ----------

export interface TokenResponse {
  access: string;
  refresh?: string;
}

export interface RegisterResponse {
  id: number;
  username: string;
}

export async function login(username: string, password: string): Promise<TokenResponse> {
  const data = await request<TokenResponse>("/auth/token", {
    method: "POST",
    body: { username, password },
  });
  localStorage.setItem("access_token", data.access);
  if (data.refresh) localStorage.setItem("refresh_token", data.refresh);
  return data;
}

export async function register(username: string, password: string, email: string): Promise<RegisterResponse> {
  return request<RegisterResponse>("/auth/register", {
    method: "POST",
    body: { username, password, email },
  });
}

export async function refreshToken(): Promise<TokenResponse> {
  const refresh = localStorage.getItem("refresh_token");
  if (!refresh) throw new Error("No refresh token");
  const data = await request<TokenResponse>("/auth/token/refresh", {
    method: "POST",
    body: { refresh },
  });
  localStorage.setItem("access_token", data.access);
  if (data.refresh) localStorage.setItem("refresh_token", data.refresh);
  return data;
}

export function logout() {
  localStorage.removeItem("access_token");
  localStorage.removeItem("refresh_token");
}

// ---------- Connections ----------

export interface ConnectionProfile {
  id: number;
  name: string;
  host: string;
  port: number;
  database_name: string;
  db_username: string;
  created_at: string;
}

export interface CreateConnectionRequest {
  name: string;
  host: string;
  port: number;
  database_name: string;
  db_username: string;
  db_password: string;
}

export function listConnections(): Promise<ConnectionProfile[]> {
  return request("/connections");
}

export function createConnection(data: CreateConnectionRequest): Promise<ConnectionProfile> {
  return request("/connections", { method: "POST", body: data });
}

export function updateConnection(id: number, data: Partial<CreateConnectionRequest>): Promise<ConnectionProfile> {
  return request(`/connections/${id}`, { method: "PUT", body: data });
}

export function deleteConnection(id: number): Promise<void> {
  return request(`/connections/${id}`, { method: "DELETE" });
}

// ---------- Query ----------

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  rowcount: number;
  status: string;
}

export function executeSQL(connectionProfileId: number, sql: string): Promise<QueryResult> {
  return request("/query/execute", {
    method: "POST",
    body: { connection_profile_id: connectionProfileId, sql },
  });
}

// ---------- Commits ----------

export interface Commit {
  version_id: string;
  seq: number;
  sql_command: string;
  commit_hash: string;
  status: string;
  timestamp: string;
  connection_profile_id?: number;
}

export function listCommits(connectionProfileId: number): Promise<Commit[]> {
  return request(`/commits?connection_profile_id=${connectionProfileId}`);
}

export function createCommit(connectionProfileId: number, sqlCommand: string): Promise<Commit> {
  return request("/commits", {
    method: "POST",
    body: { connection_profile_id: connectionProfileId, sql_command: sqlCommand },
  });
}

// ---------- Rollback ----------

export interface RollbackResult {
  rolled_back_to: string;
  snapshot_restored: string | null;
  commands_applied: number;
  status: string;
}

export function rollbackToVersion(connectionProfileId: number, targetVersionId: string): Promise<RollbackResult> {
  return request("/rollback", {
    method: "POST",
    body: { connection_profile_id: connectionProfileId, target_version_id: targetVersionId },
  });
}

// ---------- Snapshots ----------

export interface Snapshot {
  snapshot_id: string;
  version_id: string;
  s3_key: string;
  created_at: string;
  connection_profile_id: number;
}

export function listSnapshots(connectionProfileId: number): Promise<Snapshot[]> {
  return request(`/snapshots?connection_profile_id=${connectionProfileId}`);
}

export function createSnapshot(connectionProfileId: number): Promise<Snapshot> {
  return request(`/snapshots/manual?connection_profile_id=${connectionProfileId}`, {
    method: "POST",
  });
}

export function getSnapshotFrequency(connectionProfileId: number): Promise<number> {
  return request<{ frequency: number }>(
    `/snapshots/frequency?connection_profile_id=${connectionProfileId}`
  ).then((r) => r.frequency);
}

export function setSnapshotFrequency(connectionProfileId: number, frequency: number): Promise<number> {
  return request<{ frequency: number }>("/snapshots/frequency", {
    method: "PUT",
    body: { connection_profile_id: connectionProfileId, frequency },
  }).then((r) => r.frequency);
}

// ---------- Anti-commands ----------

export interface AntiCommand {
  version_id: string;
  inverse_sql: string;
  commit_version_id: string;
}

export function listAntiCommands(connectionProfileId: number): Promise<AntiCommand[]> {
  return request(`/anticommands?connection_profile_id=${connectionProfileId}`);
}

// ---------- Terminal ----------

export function getTerminalTicket(connectionProfileId: number): Promise<string> {
  return request<{ ticket: string }>(
    `/terminal/ticket?connection_profile_id=${connectionProfileId}`,
    { method: "POST" },
  ).then((r) => r.ticket);
}
