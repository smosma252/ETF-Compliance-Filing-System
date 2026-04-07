import { Link } from "wouter";
import { Activity } from "lucide-react";
import {Button} from '../components/Button'
import DashboardLayout from "../components/DashboardLayout"


export const mockPipelineJobs = [
  { id: 'pj1', jobId: 'job-001', importType: 'n_port', sourceFile: 'nport_q1_2024.tsv', status: 'completed', totalRows: 3842, processedRows: 3842, chunkSize: 50000, chunksTotal: 1, chunksCompleted: 1, exceptionsCount: 4, startedAt: '2024-04-02T09:14:00Z', completedAt: '2024-04-02T09:15:30Z', durationMs: 90000 },
  { id: 'pj2', jobId: 'job-002', importType: 'fund_info', sourceFile: 'fund_info_q1_2024.tsv', status: 'completed', totalRows: 498, processedRows: 498, chunkSize: 50000, chunksTotal: 1, chunksCompleted: 1, exceptionsCount: 2, startedAt: '2024-03-31T11:18:00Z', completedAt: '2024-03-31T11:20:00Z', durationMs: 120000 },
  { id: 'pj3', jobId: 'job-003', importType: 'n_port', sourceFile: 'nport_q4_2023.tsv', status: 'completed', totalRows: 3801, processedRows: 3801, chunkSize: 50000, chunksTotal: 1, chunksCompleted: 1, exceptionsCount: 1, startedAt: '2024-01-03T09:00:00Z', completedAt: '2024-01-03T09:01:45Z', durationMs: 105000 },
  { id: 'pj4', jobId: 'job-004', importType: 'n_port', sourceFile: 'nport_q1_2024_schwab.tsv', status: 'running', totalRows: 2500, processedRows: 1200, chunkSize: 50000, chunksTotal: 1, chunksCompleted: 0, exceptionsCount: 0, startedAt: '2024-04-02T11:00:00Z' },
  { id: 'pj5', jobId: 'job-005', importType: 'fund_info', sourceFile: 'fund_info_fidelity.tsv', status: 'queued', totalRows: 0, processedRows: 0, chunkSize: 50000, chunksTotal: 0, chunksCompleted: 0, exceptionsCount: 0, startedAt: '2024-04-02T11:05:00Z' },
];

function Home(){
    const activeJob = mockPipelineJobs.find((job) => job.status == 'running');


    return (
        <DashboardLayout title="Dashboard" subtitle="ETF Compliance Filing System — Overview">
            {/* Hero Banner */}
            <div
                className="relative h-40 overflow-hidden"
                style={{ backgroundImage: `url()`, backgroundSize: 'cover', backgroundPosition: 'center 40%' }}
            >
                <div className="absolute inset-0 bg-linear-to-r from-background via-background/80 to-background/20" />
                    <div className="absolute inset-0 bg-linear-to-t from-background/60 to-transparent" />
                    <div className="relative h-full flex items-center px-8">
                    <div>
                        <h2 className="text-2xl font-bold text-white mb-1" style={{ fontFamily: 'var(--font-display)' }}>
                        Filing Cycle
                        </h2>
                        <p className="text-sm text-white/70">
                        Last ingestion: 0 · 0 active job
                        </p>
                    </div>
                    <div className="ml-auto flex items-center gap-3">
                        {activeJob && (
                        <div className="flex items-center gap-2 bg-blue-500/15 border border-blue-500/25 rounded-lg px-3 py-2">
                            <Activity className="w-3.5 h-3.5 text-blue-400 animate-pulse" />
                            <div>
                            <p className="text-xs font-medium text-blue-300" style={{ fontFamily: 'var(--font-display)' }}>
                                {activeJob.sourceFile}
                            </p>
                            <p className="text-[10px] text-blue-400/70">
                                {activeJob.processedRows.toLocaleString()} / {activeJob.totalRows.toLocaleString()} rows
                            </p>
                            </div>
                        </div>
                        )}
                        <Link href="/upload">
                            <Button className="bg-primary hover:bg-primary/90 text-white text-xs">
                                New Import
                            </Button>
                        </Link>
                    </div>
                </div>
            </div>
        </DashboardLayout>
    )
}

export default Home;