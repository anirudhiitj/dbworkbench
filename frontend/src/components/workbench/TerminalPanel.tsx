import { useState, useRef, useEffect } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import { Terminal } from "lucide-react";

export function TerminalPanel() {
  const { activeConnection, terminalHistory, addTerminalLine, executeQuery } = useWorkspace();
  const [input, setInput] = useState("");
  const [commandHistory, setCommandHistory] = useState<string[]>([]);
  const [historyIndex, setHistoryIndex] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [terminalHistory]);

  const handleSubmit = async () => {
    if (!input.trim()) return;
    const cmd = input.trim();
    
    addTerminalLine(`${activeConnection?.database_name || "db"}=> ${cmd}`);
    setCommandHistory(prev => [...prev, cmd]);
    setHistoryIndex(-1);
    setInput("");

    if (cmd === "\\q" || cmd === "exit") {
      addTerminalLine("Bye!");
      return;
    }
    if (cmd === "\\dt" || cmd === "\\d" || cmd === "help" || cmd === "\\?") {
      addTerminalLine("Hint: use the SQL editor or type SQL directly here.");
      addTerminalLine("");
      addTerminalLine(`${activeConnection?.database_name || "db"}=> `);
      return;
    }

    // Execute as SQL
    try {
      const result = await executeQuery(cmd);
      if (result.status === "error") {
        addTerminalLine(`ERROR: ${result.rows[0]?.[0]}`);
      } else if (result.columns.length > 0) {
        const header = result.columns.join(" | ");
        addTerminalLine(header);
        addTerminalLine("-".repeat(header.length));
        result.rows.forEach(row => {
          addTerminalLine(row.map(c => c === null ? "[null]" : String(c)).join(" | "));
        });
        addTerminalLine(`(${result.rowcount} rows)`);
      } else {
        addTerminalLine(`OK (${result.rowcount} rows affected)`);
      }
    } catch (err: any) {
      addTerminalLine(`ERROR: ${err.message || "Query failed"}`);
    }
    addTerminalLine("");
    addTerminalLine(`${activeConnection?.database_name || "db"}=> `);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      void handleSubmit();
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      if (commandHistory.length > 0) {
        const newIndex = historyIndex === -1 ? commandHistory.length - 1 : Math.max(0, historyIndex - 1);
        setHistoryIndex(newIndex);
        setInput(commandHistory[newIndex]);
      }
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      if (historyIndex >= 0) {
        const newIndex = historyIndex + 1;
        if (newIndex >= commandHistory.length) {
          setHistoryIndex(-1);
          setInput("");
        } else {
          setHistoryIndex(newIndex);
          setInput(commandHistory[newIndex]);
        }
      }
    }
  };

  return (
    <div className="flex flex-col h-full" style={{ background: "hsl(var(--terminal-bg))" }}>
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-1.5 border-b border-border shrink-0 bg-toolbar-bg">
        <div className="flex items-center gap-2">
          <Terminal className="w-3.5 h-3.5 text-primary" />
          <span className="text-[11px] font-semibold text-foreground">Terminal</span>
          {activeConnection && (
            <span className="text-[10px] text-muted-foreground font-mono ml-1 bg-secondary px-1.5 py-0.5 rounded">
              psql · {activeConnection.database_name}
            </span>
          )}
        </div>
      </div>

      {/* Terminal body */}
      <div
        ref={scrollRef}
        className="flex-1 overflow-y-auto px-3 py-2 font-mono text-[12px] leading-[1.7] cursor-text min-h-0"
        style={{ color: "hsl(var(--terminal-fg))" }}
        onClick={() => inputRef.current?.focus()}
      >
        {!activeConnection ? (
          <div className="text-muted-foreground flex items-center gap-2 py-4">
            <Terminal className="w-4 h-4" />
            Select a connection to start a psql session.
          </div>
        ) : (
          <>
            {terminalHistory.map((line, i) => (
              <div key={i} className="whitespace-pre">{line}</div>
            ))}
            <div className="flex items-center">
              <input
                ref={inputRef}
                value={input}
                onChange={e => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                className="flex-1 bg-transparent outline-none caret-primary font-mono text-[12px]"
                style={{ color: "hsl(var(--terminal-fg))" }}
                spellCheck={false}
                autoFocus
              />
            </div>
          </>
        )}
      </div>
    </div>
  );
}
