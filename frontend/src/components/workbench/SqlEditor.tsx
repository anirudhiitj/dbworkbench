import { useState, useCallback, useRef, useEffect } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import { Play, Plus, X, Loader2, FileCode2 } from "lucide-react";

interface QueryTab {
  id: string;
  title: string;
  sql: string;
}

let tabCounter = 1;
function newTab(sql = ""): QueryTab {
  return { id: `tab-${tabCounter++}`, title: `Query ${tabCounter - 1}`, sql };
}

export function SqlEditor() {
  const { activeConnection, executeQuery } = useWorkspace();
  const [tabs, setTabs] = useState<QueryTab[]>(() => [newTab("SELECT * FROM public.users LIMIT 100;")]);
  const [activeTabId, setActiveTabId] = useState(tabs[0].id);
  const [running, setRunning] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const activeTab = tabs.find((t) => t.id === activeTabId) ?? tabs[0];

  const updateTabSql = useCallback((sql: string) => {
    setTabs(prev => prev.map(t => t.id === activeTabId ? { ...t, sql } : t));
  }, [activeTabId]);

  const addTab = useCallback((sql = "") => {
    const t = newTab(sql);
    setTabs(prev => [...prev, t]);
    setActiveTabId(t.id);
  }, []);

  const closeTab = useCallback((id: string) => {
    setTabs(prev => {
      if (prev.length <= 1) return prev;
      const filtered = prev.filter(t => t.id !== id);
      if (activeTabId === id) {
        setActiveTabId(filtered[filtered.length - 1].id);
      }
      return filtered;
    });
  }, [activeTabId]);

  const handleRun = useCallback(async () => {
    if (!activeConnection || !activeTab.sql.trim()) return;
    setRunning(true);
    try {
      await executeQuery(activeTab.sql);
    } catch (err: any) {
      console.error("Query error:", err.message);
    } finally {
      setRunning(false);
    }
  }, [activeConnection, activeTab.sql, executeQuery]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
        e.preventDefault();
        handleRun();
      }
      if (e.key === "F5") {
        e.preventDefault();
        handleRun();
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [handleRun]);

  const lineCount = activeTab.sql.split("\n").length;

  return (
    <div className="flex flex-col h-full bg-editor-bg">
      {/* Tab bar + Run button */}
      <div className="flex items-center justify-between border-b border-border shrink-0 bg-toolbar-bg">
        <div className="flex items-center overflow-x-auto min-w-0">
          {tabs.map(tab => (
            <button
              key={tab.id}
              className={`group flex items-center gap-1.5 px-3 py-2 text-[11px] cursor-pointer border-r border-border transition-all whitespace-nowrap ${
                tab.id === activeTabId
                  ? "bg-editor-bg text-foreground border-b-2 border-b-primary -mb-px"
                  : "text-muted-foreground hover:text-foreground hover:bg-accent/50 border-b-2 border-b-transparent -mb-px"
              }`}
              onClick={() => setActiveTabId(tab.id)}
            >
              <FileCode2 className="w-3 h-3 text-primary/60 shrink-0" />
              <span>{tab.title}</span>
              {tabs.length > 1 && (
                <span
                  onClick={(e) => { e.stopPropagation(); closeTab(tab.id); }}
                  className="ml-1 opacity-0 group-hover:opacity-100 hover:text-destructive transition-all rounded-sm hover:bg-destructive/10 p-0.5"
                >
                  <X className="w-2.5 h-2.5" />
                </span>
              )}
            </button>
          ))}
          <button
            onClick={() => addTab()}
            className="px-2.5 py-2 text-muted-foreground hover:text-foreground hover:bg-accent/50 transition-colors"
            title="New query tab (Ctrl+T)"
          >
            <Plus className="w-3.5 h-3.5" />
          </button>
        </div>

        {/* Run button */}
        <div className="px-3 shrink-0 flex items-center gap-2">
          <span className="text-[9px] text-muted-foreground/60 hidden sm:inline">Ctrl+Enter</span>
          <button
            onClick={handleRun}
            disabled={running || !activeConnection}
            className="flex items-center gap-1.5 rounded-md px-3 py-1.5 text-[11px] font-semibold transition-all disabled:opacity-40 disabled:cursor-not-allowed bg-success text-white hover:bg-success/90 shadow-sm active:scale-[0.97]"
          >
            {running ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
            {running ? "Running…" : "Run"}
          </button>
        </div>
      </div>

      {/* Editor area */}
      <div className="flex-1 relative min-h-0 overflow-hidden">
        {/* Line numbers gutter */}
        <div className="absolute left-0 top-0 bottom-0 w-11 bg-sidebar-bg border-r border-border flex flex-col pt-3 select-none overflow-hidden z-10">
          {Array.from({ length: Math.max(lineCount, 20) }, (_, i) => (
            <div key={i} className={`text-[11px] text-right pr-3 leading-[1.6] font-mono ${
              i < lineCount ? "text-muted-foreground/50" : "text-transparent"
            }`}>
              {i + 1}
            </div>
          ))}
        </div>
        <textarea
          ref={textareaRef}
          value={activeTab.sql}
          onChange={(e) => updateTabSql(e.target.value)}
          className="w-full h-full resize-none bg-transparent text-foreground text-[13px] font-mono leading-[1.6] p-3 pl-14 outline-none"
          spellCheck={false}
          placeholder="Enter SQL query..."
        />
      </div>
    </div>
  );
}
