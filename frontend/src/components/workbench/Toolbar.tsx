import { useState } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import { useAuth } from "@/lib/auth-context";
import { useTheme } from "@/hooks/use-theme";
import { Database, Plus, Pencil, LogOut, Zap, ZapOff, Sun, Moon, X, Loader2, ChevronDown } from "lucide-react";

export function Toolbar() {
  const { activeConnection, connections, setActiveConnection, addConnection, refreshConnections } = useWorkspace();
  const { username, logout } = useAuth();
  const { theme, toggleTheme } = useTheme();
  const [showNewConn, setShowNewConn] = useState(false);
  const [saving, setSaving] = useState(false);
  const [form, setForm] = useState({ name: "", host: "localhost", port: "5432", database_name: "", db_username: "postgres", db_password: "" });

  const handleCreateConnection = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    try {
      const conn = await addConnection({ ...form, port: Number(form.port) });
      setActiveConnection(conn);
      setShowNewConn(false);
      setForm({ name: "", host: "localhost", port: "5432", database_name: "", db_username: "postgres", db_password: "" });
    } catch (err: any) {
      alert(err.message || "Failed to create connection");
    } finally {
      setSaving(false);
    }
  };

  return (
    <>
      <div className="flex items-center justify-between h-11 px-3 bg-toolbar-bg border-b border-toolbar-border shrink-0 select-none">
        {/* Left: Logo + Connection */}
        <div className="flex items-center gap-3">
          {/* Logo */}
          <div className="flex items-center gap-2">
            <div className="w-6 h-6 rounded-md bg-primary/15 flex items-center justify-center">
              <Database className="w-3.5 h-3.5 text-primary" />
            </div>
            <span className="text-[13px] font-bold tracking-tight text-foreground">WEAVE-DB</span>
          </div>

          {/* Separator */}
          <div className="w-px h-5 bg-border" />

          {/* Connection selector */}
          <div className="relative">
            <select
              value={activeConnection?.id ?? ""}
              onChange={(e) => {
                const id = Number(e.target.value);
                const conn = connections.find((c) => c.id === id) ?? null;
                setActiveConnection(conn);
              }}
              className="appearance-none bg-secondary border border-border text-foreground text-[11px] rounded-md pl-2.5 pr-7 py-1.5 min-w-[200px] outline-none cursor-pointer focus:ring-1 focus:ring-primary/50 focus:border-primary/50 transition-all"
            >
              <option value="">— Select connection —</option>
              {connections.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.name} ({c.db_username}@{c.host}/{c.database_name})
                </option>
              ))}
            </select>
            <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3 h-3 text-muted-foreground pointer-events-none" />
          </div>

          {/* Status indicator */}
          {activeConnection ? (
            <div className="flex items-center gap-1.5 text-[10px] font-medium">
              <div className="w-1.5 h-1.5 rounded-full bg-success animate-pulse" />
              <span className="text-success">Connected</span>
            </div>
          ) : (
            <div className="flex items-center gap-1.5 text-muted-foreground text-[10px] font-medium">
              <div className="w-1.5 h-1.5 rounded-full bg-muted-foreground/50" />
              <span>Disconnected</span>
            </div>
          )}

          {/* Action buttons */}
          <div className="w-px h-4 bg-border" />
          <button
            onClick={() => setShowNewConn(true)}
            className="flex items-center gap-1 text-[10px] text-primary hover:text-primary/80 transition-colors font-semibold px-1.5 py-1 rounded hover:bg-primary/10"
          >
            <Plus className="w-3 h-3" />
            New
          </button>
          {activeConnection && (
            <button className="flex items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground transition-colors px-1.5 py-1 rounded hover:bg-accent">
              <Pencil className="w-3 h-3" />
              Edit
            </button>
          )}
        </div>

        {/* Right: Theme toggle + User info */}
        <div className="flex items-center gap-2">
          <button
            onClick={toggleTheme}
            className="p-1 rounded hover:bg-accent transition-colors text-muted-foreground hover:text-foreground"
            title={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
          >
            {theme === "dark" ? <Sun className="w-3.5 h-3.5" /> : <Moon className="w-3.5 h-3.5" />}
          </button>
          <div className="w-px h-4 bg-border" />
          <div className="w-6 h-6 rounded-full bg-primary/20 text-primary flex items-center justify-center text-[10px] font-bold">
            {(username || "U")[0].toUpperCase()}
          </div>
          <span className="text-xs text-muted-foreground">{username || "User"}</span>
          <button
            onClick={logout}
            className="flex items-center gap-1 text-[10px] text-muted-foreground hover:text-destructive transition-colors ml-2"
            title="Logout"
          >
            <LogOut className="w-3 h-3" />
          </button>
        </div>
      </div>

      {/* New Connection Modal */}
      {showNewConn && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
          <div className="bg-card border border-border rounded-lg shadow-xl w-full max-w-md p-6 relative">
            <button onClick={() => setShowNewConn(false)} className="absolute top-3 right-3 text-muted-foreground hover:text-foreground">
              <X className="w-4 h-4" />
            </button>
            <h2 className="text-sm font-bold text-foreground mb-4">New Connection</h2>
            <form onSubmit={handleCreateConnection} className="space-y-3">
              {(["name", "host", "port", "database_name", "db_username", "db_password"] as const).map((field) => (
                <div key={field}>
                  <label className="block text-[10px] font-medium text-muted-foreground mb-0.5 capitalize">{field.replace(/_/g, " ")}</label>
                  <input
                    type={field === "db_password" ? "password" : field === "port" ? "number" : "text"}
                    value={form[field]}
                    onChange={(e) => setForm((f) => ({ ...f, [field]: e.target.value }))}
                    className="w-full px-2 py-1.5 text-xs bg-muted border border-border rounded outline-none focus:ring-1 focus:ring-primary text-foreground"
                    required
                  />
                </div>
              ))}
              <button
                type="submit"
                disabled={saving}
                className="w-full flex items-center justify-center gap-2 py-2 text-xs font-semibold rounded bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              >
                {saving && <Loader2 className="w-3 h-3 animate-spin" />}
                Create Connection
              </button>
            </form>
          </div>
        </div>
      )}
    </>
  );
}
