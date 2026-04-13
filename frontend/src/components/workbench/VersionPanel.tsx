import { useState } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import { GitCommit, Camera, RotateCcw, ChevronDown, ChevronRight, RefreshCw, Copy, Settings, Loader2, Clock, GitBranch } from "lucide-react";

type Tab = "commits" | "snapshots";

function relTime(ts: string) {
  const diff = Date.now() - new Date(ts).getTime();
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

function fmtDate(ts: string) {
  return new Date(ts).toLocaleString(undefined, {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
  });
}

export function VersionPanel() {
  const { activeConnection, commits, snapshots, rollback, refreshCommits, refreshSnapshots, createSnapshot, snapshotFrequency } = useWorkspace();
  const [tab, setTab] = useState<Tab>("commits");
  const [expandedCommits, setExpandedCommits] = useState<Set<string>>(new Set());
  const [rollingBack, setRollingBack] = useState<string | null>(null);
  const [creatingSnapshot, setCreatingSnapshot] = useState(false);

  const handleRollback = async (versionId: string) => {
    if (!activeConnection) return;
    setRollingBack(versionId);
    try {
      await rollback(versionId);
      await refreshSnapshots();
    } catch (err: any) {
      alert(err.message || "Rollback failed");
    } finally {
      setRollingBack(null);
    }
  };

  const handleSnapshot = async () => {
    if (!activeConnection) return;
    setCreatingSnapshot(true);
    try {
      await createSnapshot();
    } catch (err: any) {
      alert(err.message || "Snapshot failed");
    } finally {
      setCreatingSnapshot(false);
    }
  };

  const toggleExpand = (id: string) => {
    setExpandedCommits(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const copySQL = (sql: string) => navigator.clipboard?.writeText(sql).catch(() => {});

  return (
    <div className="flex flex-col h-full bg-panel-bg">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-border bg-toolbar-bg">
        <div className="flex items-center gap-2">
          <GitBranch className="w-3.5 h-3.5 text-primary" />
          <span className="text-[11px] font-semibold text-foreground uppercase tracking-wider">Version Control</span>
        </div>
        <div className="flex items-center gap-1">
          {activeConnection && tab === "snapshots" && (
            <button
              onClick={handleSnapshot}
              disabled={creatingSnapshot}
              className="flex items-center gap-1 text-[10px] text-primary hover:text-primary/80 transition-colors font-semibold px-1.5 py-1 rounded hover:bg-primary/10 disabled:opacity-60"
            >
              {creatingSnapshot ? <Loader2 className="w-3 h-3 animate-spin" /> : <Camera className="w-3 h-3" />}
              {creatingSnapshot ? "Creating..." : "Snapshot"}
            </button>
          )}
          <button
            onClick={async () => {
              await refreshCommits();
              await refreshSnapshots();
            }}
            className="text-muted-foreground hover:text-foreground transition-colors p-0.5 rounded hover:bg-accent"
          >
            <RefreshCw className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-border shrink-0 bg-toolbar-bg">
        {(["commits", "snapshots"] as const).map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`flex-1 flex items-center justify-center gap-1.5 py-2 text-[11px] font-semibold capitalize transition-all ${
              tab === t
                ? "text-foreground border-b-2 border-b-primary -mb-px"
                : "text-muted-foreground border-b-2 border-b-transparent hover:text-foreground -mb-px"
            }`}
          >
            {t === "commits" ? <GitCommit className="w-3 h-3" /> : <Camera className="w-3 h-3" />}
            {t}
            {t === "commits" && commits.length > 0 && (
              <span className="text-[9px] bg-primary/15 text-primary px-1.5 py-0.5 rounded-full font-bold tabular-nums">
                {commits.length}
              </span>
            )}
          </button>
        ))}
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto min-h-0">
        {!activeConnection && (
          <div className="px-3 py-8 text-center text-[11px] text-muted-foreground flex flex-col items-center gap-2">
            <GitBranch className="w-6 h-6 text-muted-foreground/30" />
            Select a connection to view history
          </div>
        )}

        {/* COMMITS TAB */}
        {tab === "commits" && activeConnection && (
          <div className="py-1">
            {commits.length === 0 ? (
              <div className="px-3 py-8 text-center text-[11px] text-muted-foreground">No commits yet</div>
            ) : (
              /* Timeline */
              <div className="relative">
                {/* Timeline line */}
                <div className="absolute left-[18px] top-4 bottom-4 w-px bg-border" />
                
                {[...commits].reverse().map((c, idx) => {
                  const isExpanded = expandedCommits.has(c.version_id);
                  return (
                    <div key={c.version_id} className="animate-fade-in relative" style={{ animationDelay: `${idx * 30}ms` }}>
                      <button
                        onClick={() => toggleExpand(c.version_id)}
                        className={`w-full text-left px-3 py-2.5 transition-colors ${
                          isExpanded ? "bg-accent" : "hover:bg-sidebar-hover"
                        }`}
                      >
                        <div className="flex items-start gap-2.5">
                          {/* Timeline dot */}
                          <div className="relative z-10 mt-0.5">
                            <div className={`w-2 h-2 rounded-full ring-2 ring-panel-bg ${idx === 0 ? "bg-primary" : "bg-version-dot/60"}`} />
                          </div>
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center justify-between">
                              <span className="text-[11px] font-mono font-bold text-primary">v{c.seq}</span>
                              <span className="text-[10px] text-muted-foreground/70 flex items-center gap-1">
                                <Clock className="w-2.5 h-2.5" />
                                {relTime(c.timestamp)}
                              </span>
                            </div>
                            <p className="text-[10px] text-foreground/60 mt-0.5 font-mono truncate">{c.sql_command}</p>
                            {c.commit_hash && (
                              <p className="text-[9px] text-muted-foreground/50 mt-0.5 font-mono truncate" title={c.commit_hash}>
                                #{c.commit_hash.slice(0, 8)}
                              </p>
                            )}
                          </div>
                        </div>
                      </button>


                      {isExpanded && (
                        <div className="mx-3 ml-8 mb-2 p-2.5 bg-secondary/50 border border-border rounded-md space-y-2 animate-fade-in">
                          {c.commit_hash && (
                            <div className="flex items-center gap-1.5 mb-2">
                              <span className="text-[9px] font-semibold text-muted-foreground uppercase tracking-wider">Hash</span>
                              <code className="text-[9px] font-mono text-foreground/70 bg-editor-bg px-1.5 py-0.5 rounded border border-border select-all">{c.commit_hash}</code>
                            </div>
                          )}
                          <div>
                            <div className="flex items-center justify-between mb-1">
                              <span className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">SQL</span>
                              <button
                                onClick={() => copySQL(c.sql_command)}
                                className="text-muted-foreground hover:text-foreground transition-colors p-0.5 rounded hover:bg-accent"
                              >
                                <Copy className="w-3 h-3" />
                              </button>
                            </div>
                            <pre className="text-[10px] font-mono text-foreground/80 bg-editor-bg rounded-md p-2 overflow-x-auto whitespace-pre-wrap border border-border">
                              {c.sql_command}
                            </pre>
                          </div>
                          <button
                            onClick={() => handleRollback(c.version_id)}
                            disabled={rollingBack === c.version_id}
                            className="flex items-center gap-1 text-[10px] font-medium text-destructive/80 hover:text-destructive transition-colors bg-destructive/10 hover:bg-destructive/15 px-2 py-1 rounded-md border border-destructive/20 disabled:opacity-60"
                          >
                            {rollingBack === c.version_id ? <Loader2 className="w-3 h-3 animate-spin" /> : <RotateCcw className="w-3 h-3" />}
                            {rollingBack === c.version_id ? "Rolling back..." : `Rollback to v${c.seq}`}
                          </button>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}

        {/* SNAPSHOTS TAB */}
        {tab === "snapshots" && activeConnection && (
          <div className="py-1">
            <div className="px-3 py-2 border-b border-border">
              <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground">
                <Settings className="w-3 h-3" />
                Auto-snapshot every <span className="font-bold text-foreground">{snapshotFrequency}</span> commits
              </div>
            </div>

            {snapshots.length === 0 ? (
              <div className="px-3 py-8 text-center text-[11px] text-muted-foreground">
                No snapshots yet
              </div>
            ) : (
              snapshots.map((s, idx) => (
                <div
                  key={s.snapshot_id}
                  className="px-3 py-2.5 hover:bg-sidebar-hover transition-colors border-b border-border animate-fade-in"
                  style={{ animationDelay: `${idx * 30}ms` }}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <Camera className="w-3.5 h-3.5 text-primary/70" />
                      <span className="text-[11px] font-mono font-semibold text-foreground">{s.snapshot_id.slice(0, 12)}</span>
                    </div>
                    <span className="text-[10px] text-muted-foreground/70">{relTime(s.created_at)}</span>
                  </div>
                  <p className="text-[10px] text-muted-foreground mt-0.5 pl-[22px]">{fmtDate(s.created_at)}</p>
                </div>
              ))
            )}
          </div>
        )}
      </div>
    </div>
  );
}
