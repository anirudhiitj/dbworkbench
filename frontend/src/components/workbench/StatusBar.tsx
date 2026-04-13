import { useWorkspace } from "@/lib/workspace-context";
import { Zap, ZapOff, GitCommit } from "lucide-react";

export function StatusBar() {
  const { activeConnection, commits } = useWorkspace();

  return (
    <div className="flex items-center justify-between h-[22px] px-3 bg-status-bar-bg text-status-bar-fg text-[10px] shrink-0 select-none">
      <div className="flex items-center gap-3">
        {activeConnection ? (
          <>
            <span className="flex items-center gap-1">
              <Zap className="w-3 h-3" />
              Connected
            </span>
            <span className="font-mono opacity-80">
              {activeConnection.db_username}@{activeConnection.host}:{activeConnection.port}/{activeConnection.database_name}
            </span>
          </>
        ) : (
          <span className="flex items-center gap-1 opacity-70">
            <ZapOff className="w-3 h-3" />
            No connection
          </span>
        )}
      </div>
      <div className="flex items-center gap-3">
        {activeConnection && (
          <span className="flex items-center gap-1 opacity-80">
            <GitCommit className="w-3 h-3" />
            {commits.length} commits
          </span>
        )}
        <span className="opacity-60">UTF-8</span>
        <span className="opacity-60">PostgreSQL 16</span>
        <span className="opacity-40">WEAVE-DB v0.3</span>
      </div>
    </div>
  );
}
