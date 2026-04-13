import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";
import { Toolbar } from "@/components/workbench/Toolbar";
import { DatabaseSidebar } from "@/components/workbench/DatabaseSidebar";
import { SqlEditor } from "@/components/workbench/SqlEditor";
import { ResultsGrid } from "@/components/workbench/ResultsGrid";
import { TerminalPanel } from "@/components/workbench/TerminalPanel";
import { VersionPanel } from "@/components/workbench/VersionPanel";
import { StatusBar } from "@/components/workbench/StatusBar";
import { WorkspaceProvider } from "@/lib/workspace-context";

function CenterWorkspace() {
  return (
    <div className="flex flex-col h-full w-full">
      {/* SQL Editor - top 40% */}
      <div className="flex-[4] min-h-0 border-b border-border">
        <SqlEditor />
      </div>
      {/* Results Grid - middle 35% */}
      <div className="flex-[3.5] min-h-0 border-b border-border">
        <ResultsGrid />
      </div>
      {/* Terminal - bottom 25% */}
      <div className="flex-[2.5] min-h-0">
        <TerminalPanel />
      </div>
    </div>
  );
}

function WorkbenchLayout() {
  return (
    <div className="flex flex-col h-screen w-screen overflow-hidden bg-background">
      <Toolbar />
      
      <div className="flex-1 min-h-0 overflow-hidden">
        <PanelGroup direction="horizontal">
          <Panel defaultSize={17} minSize={12} maxSize={28}>
            <DatabaseSidebar />
          </Panel>
          
          <PanelResizeHandle className="w-[3px] bg-border hover:bg-primary/60 transition-colors" />
          
          <Panel defaultSize={60} minSize={35}>
            <CenterWorkspace />
          </Panel>
          
          <PanelResizeHandle className="w-[3px] bg-border hover:bg-primary/60 transition-colors" />
          
          <Panel defaultSize={23} minSize={14} maxSize={32}>
            <VersionPanel />
          </Panel>
        </PanelGroup>
      </div>
      
      <StatusBar />
    </div>
  );
}

export function WorkbenchShell() {
  return (
    <WorkspaceProvider>
      <WorkbenchLayout />
    </WorkspaceProvider>
  );
}
