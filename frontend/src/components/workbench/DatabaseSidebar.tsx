import { useState } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import { ChevronRight, ChevronDown, Table2, Eye, Zap, Hash, Key, RefreshCw, FolderOpen, Columns3, Search } from "lucide-react";

export function DatabaseSidebar() {
  const { activeConnection, schemas } = useWorkspace();
  const [expanded, setExpanded] = useState<Set<string>>(new Set(["public", "public.Tables"]));
  const [selectedItem, setSelectedItem] = useState<string | null>(null);
  const [search, setSearch] = useState("");

  const toggle = (key: string) => {
    setExpanded(prev => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const iconFor = (type: string) => {
    switch (type) {
      case "table": return <Table2 className="w-3.5 h-3.5 text-primary/70" />;
      case "view": return <Eye className="w-3.5 h-3.5 text-success/70" />;
      case "function": return <Zap className="w-3.5 h-3.5 text-warning/70" />;
      case "sequence": return <Hash className="w-3.5 h-3.5 text-muted-foreground" />;
      default: return <Columns3 className="w-3.5 h-3.5" />;
    }
  };

  return (
    <div className="flex flex-col h-full bg-sidebar-bg">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-border bg-toolbar-bg">
        <span className="text-[11px] font-semibold text-foreground uppercase tracking-wider">Explorer</span>
        {activeConnection && (
          <button className="text-muted-foreground hover:text-foreground transition-colors p-0.5 rounded hover:bg-accent" title="Refresh">
            <RefreshCw className="w-3.5 h-3.5" />
          </button>
        )}
      </div>

      {/* Search */}
      {activeConnection && (
        <div className="px-2 py-1.5 border-b border-border">
          <div className="flex items-center gap-1.5 bg-secondary rounded-md px-2 py-1">
            <Search className="w-3 h-3 text-muted-foreground shrink-0" />
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search objects..."
              className="flex-1 bg-transparent text-[11px] text-foreground outline-none placeholder:text-muted-foreground/50"
            />
          </div>
        </div>
      )}

      {/* Connection info */}
      {activeConnection && (
        <div className="px-3 py-2 border-b border-border">
          <div className="text-[11px] font-semibold text-foreground flex items-center gap-1.5">
            <div className="w-1.5 h-1.5 rounded-full bg-success" />
            {activeConnection.database_name}
          </div>
          <div className="text-[10px] text-muted-foreground mt-0.5">{activeConnection.db_username}@{activeConnection.host}:{activeConnection.port}</div>
        </div>
      )}

      {/* Tree */}
      <div className="flex-1 overflow-y-auto py-1 min-h-0">
        {!activeConnection && (
          <div className="px-3 py-8 text-center text-[11px] text-muted-foreground">
            Select a connection to browse
          </div>
        )}

        {schemas.map((schema) => {
          const sKey = schema.name;
          const sOpen = expanded.has(sKey);

          const categories = [
            { name: "Tables", type: "table" as const, items: schema.tables },
            { name: "Views", type: "view" as const, items: schema.views },
            { name: "Functions", type: "function" as const, items: schema.functions },
            { name: "Sequences", type: "sequence" as const, items: schema.sequences },
          ];

          return (
            <div key={sKey}>
              <button
                onClick={() => toggle(sKey)}
                className="w-full flex items-center gap-1.5 px-2 py-[4px] text-left text-[11px] hover:bg-sidebar-hover transition-colors text-foreground group"
              >
                {sOpen ? <ChevronDown className="w-3 h-3 text-muted-foreground" /> : <ChevronRight className="w-3 h-3 text-muted-foreground" />}
                <FolderOpen className="w-3.5 h-3.5 text-warning/80" />
                <span className="font-semibold">{schema.name}</span>
              </button>

              {sOpen && categories.map((cat) => {
                const cKey = `${sKey}.${cat.name}`;
                const cOpen = expanded.has(cKey);
                const filteredItems = search
                  ? cat.items.filter(item => item.name.toLowerCase().includes(search.toLowerCase()))
                  : cat.items;

                if (search && filteredItems.length === 0) return null;

                return (
                  <div key={cKey}>
                    <button
                      onClick={() => toggle(cKey)}
                      className="w-full flex items-center gap-1.5 pl-6 pr-2 py-[4px] text-left text-[11px] hover:bg-sidebar-hover transition-colors text-foreground"
                    >
                      {cOpen ? <ChevronDown className="w-3 h-3 text-muted-foreground" /> : <ChevronRight className="w-3 h-3 text-muted-foreground" />}
                      <FolderOpen className="w-3.5 h-3.5 text-muted-foreground/60" />
                      <span>{cat.name}</span>
                      <span className="text-[10px] text-muted-foreground/50 ml-auto tabular-nums">{filteredItems.length}</span>
                    </button>

                    {cOpen && filteredItems.map((item) => {
                      const itemName = item.name;
                      const iKey = `${cKey}.${itemName}`;
                      const iOpen = expanded.has(iKey);
                      const isSelected = selectedItem === `${sKey}.${itemName}`;
                      const hasColumns = cat.type === "table" && "columns" in item;

                      return (
                        <div key={iKey}>
                          <button
                            onClick={() => {
                              setSelectedItem(`${sKey}.${itemName}`);
                              if (hasColumns) toggle(iKey);
                            }}
                            className={`w-full flex items-center gap-1.5 pl-10 pr-2 py-[3px] text-left text-[11px] transition-colors ${
                              isSelected ? "bg-sidebar-active text-foreground" : "hover:bg-sidebar-hover text-foreground/80"
                            }`}
                          >
                            {hasColumns && (
                              iOpen ? <ChevronDown className="w-2.5 h-2.5 text-muted-foreground shrink-0" /> : <ChevronRight className="w-2.5 h-2.5 text-muted-foreground shrink-0" />
                            )}
                            {!hasColumns && <span className="w-2.5" />}
                            {iconFor(cat.type)}
                            <span className="truncate">{itemName}</span>
                          </button>

                          {iOpen && hasColumns && "columns" in item && (item as any).columns?.map((col: any) => (
                            <div
                              key={`${iKey}.${col.name}`}
                              className="flex items-center gap-1.5 pl-[3.5rem] pr-2 py-[2px] text-[10px] text-muted-foreground hover:bg-sidebar-hover transition-colors"
                            >
                              {col.isPk ? (
                                <Key className="w-3 h-3 text-warning shrink-0" />
                              ) : (
                                <span className="w-3 text-center text-border text-[9px]">•</span>
                              )}
                              <span className="text-foreground/70">{col.name}</span>
                              <span className="ml-auto text-muted-foreground/50 font-mono text-[9px]">{col.type}</span>
                            </div>
                          ))}
                        </div>
                      );
                    })}

                    {cOpen && filteredItems.length === 0 && (
                      <div className="pl-10 pr-2 py-1 text-[10px] text-muted-foreground/50 italic">(empty)</div>
                    )}
                  </div>
                );
              })}
            </div>
          );
        })}
      </div>
    </div>
  );
}
