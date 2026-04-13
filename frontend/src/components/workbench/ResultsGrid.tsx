import { useState } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import { AlertCircle, CheckCircle2, Table2, MessageSquare } from "lucide-react";

type ResultTab = "data" | "messages";

export function ResultsGrid() {
  const { queryResult } = useWorkspace();
  const [activeTab, setActiveTab] = useState<ResultTab>("data");
  const result = queryResult;

  return (
    <div className="flex flex-col h-full bg-panel-bg">
      {/* Header tabs */}
      <div className="flex items-center justify-between border-b border-border shrink-0 bg-toolbar-bg px-1">
        <div className="flex items-center">
          {([
            { key: "data" as const, label: "Data Output", icon: Table2 },
            { key: "messages" as const, label: "Messages", icon: MessageSquare },
          ]).map(tab => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-medium transition-all ${
                activeTab === tab.key
                  ? "text-foreground border-b-2 border-b-primary -mb-px"
                  : "text-muted-foreground border-b-2 border-b-transparent hover:text-foreground -mb-px"
              }`}
            >
              <tab.icon className="w-3 h-3" />
              {tab.label}
              {tab.key === "messages" && result?.status === "error" && (
                <span className="w-1.5 h-1.5 rounded-full bg-destructive animate-pulse" />
              )}
            </button>
          ))}
        </div>
        {result?.elapsed != null && (
          <span className="text-[10px] text-muted-foreground font-mono pr-2 bg-secondary px-1.5 py-0.5 rounded">
            {result.elapsed < 1000 ? `${result.elapsed} ms` : `${(result.elapsed / 1000).toFixed(2)} s`}
          </span>
        )}
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto min-h-0">
        {!result && (
          <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-xs gap-2">
            <Table2 className="w-8 h-8 text-muted-foreground/30" />
            <span>Execute a query to see results</span>
            <span className="text-[10px] text-muted-foreground/50">Ctrl+Enter or click Run</span>
          </div>
        )}

        {result && activeTab === "messages" && (
          <div className="p-3 animate-fade-in">
            {result.status === "error" ? (
              <div className="flex items-start gap-2.5 text-xs bg-destructive/10 border border-destructive/20 rounded-md p-3">
                <AlertCircle className="w-4 h-4 text-destructive shrink-0 mt-0.5" />
                <div>
                  <span className="font-semibold text-destructive">ERROR</span>
                  <p className="text-foreground mt-1 font-mono text-[11px]">{result.rows[0]?.[0] as string}</p>
                </div>
              </div>
            ) : (
              <div className="flex items-start gap-2.5 text-xs bg-success/10 border border-success/20 rounded-md p-3">
                <CheckCircle2 className="w-4 h-4 text-success shrink-0 mt-0.5" />
                <p className="text-foreground">
                  Query returned successfully: <strong>{result.rowcount} row(s)</strong>
                  {result.elapsed != null && ` in ${result.elapsed} ms`}
                </p>
              </div>
            )}
          </div>
        )}

        {result && activeTab === "data" && result.status === "error" && (
          <div className="p-3 text-xs text-destructive font-mono animate-fade-in">
            {result.rows[0]?.[0] as string}
          </div>
        )}

        {result && activeTab === "data" && result.status === "success" && result.columns.length > 0 && (
          <div className="overflow-auto animate-fade-in">
            <table className="w-full text-[11px] font-mono border-collapse">
              <thead>
                <tr className="bg-grid-header sticky top-0 z-10">
                  <th className="px-3 py-2 text-left font-semibold text-muted-foreground border-b border-grid-border w-10 text-center">#</th>
                  {result.columns.map((col, i) => (
                    <th key={i} className="px-3 py-2 text-left font-semibold text-muted-foreground border-b border-grid-border whitespace-nowrap">
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.rows.map((row, ri) => (
                  <tr key={ri} className={`${ri % 2 === 1 ? "bg-grid-row-alt" : ""} hover:bg-sidebar-hover transition-colors`}>
                    <td className="px-3 py-1.5 text-muted-foreground/40 border-b border-grid-border text-center">{ri + 1}</td>
                    {row.map((cell, ci) => (
                      <td key={ci} className="px-3 py-1.5 border-b border-grid-border whitespace-nowrap text-foreground/90">
                        {cell === null ? (
                          <span className="text-muted-foreground/30 italic text-[10px]">NULL</span>
                        ) : (
                          String(cell)
                        )}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {result && activeTab === "data" && result.status === "success" && result.columns.length === 0 && (
          <div className="flex items-center justify-center h-full text-xs text-muted-foreground animate-fade-in">
            Query OK. {result.rowcount} row(s) affected.
          </div>
        )}
      </div>

      {/* Footer */}
      {result && result.status === "success" && (
        <div className="flex items-center gap-3 px-3 py-1 border-t border-border shrink-0 text-[10px] text-muted-foreground bg-toolbar-bg">
          <span>Rows: <strong className="text-foreground">{result.rowcount}</strong></span>
          {result.elapsed != null && <span>Time: <strong className="text-foreground">{result.elapsed} ms</strong></span>}
        </div>
      )}
    </div>
  );
}
