
import { useState, useRef } from "react";
import DashboardLayout from "../components/DashboardLayout"
import { Button } from "../components/Button";
import { Card, CardContent, CardHeader, CardTitle } from "../components/Card";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "../components/Select";
import { UploadIcon, X, CheckCircle2, AlertTriangle, Info } from "lucide-react";


interface UploadFiles {
    id: string
    file: File;
    type: 'fund_info' | 'n_port' | '';
    status: 'pending' | 'validating' | 'uploading' | 'processing' | 'done' | 'error';
    progress: number;
    rowsProcessed?: number;
    exceptionsFound?: number;
    errorMessage?: string;
}


function UploadPage () {
    const [isDragging, setIsDragging] = useState(false);
    const [files, setFiles] = useState<UploadFiles[]>([])
    const fileInputRef = useRef<HTMLInputElement>(null);

    const addFiles = (newFiles: FileList | null) => {
        if (!newFiles) {
            return;
        }
        const added: UploadFiles[] = Array.from(newFiles).map((f) => ({
            id: Math.random().toLocaleString(),
            file: f,
            type: f.name.toLowerCase().includes('fund') ? 'fund_info' : f.name.toLowerCase().includes('nport') ? 'n_port' : '',
            status: 'pending',
            progress: 0, 
        }));
        setFiles(prev => [...prev, ...added])
    }

    const handleDrop = () => {}
    const removeFile = (fileId:string) => {}
    const uploadFile = (fileId:string) => {}


    const allReady = files.length > 0 ? true : false;
    const startAll = () => {
        files.filter((f) => {
            return f.status == 'pending'
        }).forEach(f => uploadFile(f.id));
    }

    return (
        <DashboardLayout title="Upload" subtitle="uplaod all files">
            <div className="p-6 space-y-5">

                {/* Pipeline Architecture */}
                <div className="card-glow overflow-hidden">
                    <div className="grid grid-row-1 lg:grid-row-2">
                        <div className="p-5 space-y-3">
                            <h3 className="text-sm font-semibold text-foreground" style={{ fontFamily: 'var(--font-display)' }}>
                                Data Ingestion Pipeline
                            </h3>
                            <div className="flex space-y-2 text-xs text-muted-foreground">
                                {[
                                { step: '1', label: 'Upload API (FastAPI)', desc: 'Receives tab-delimited TSV files' },
                                { step: '2', label: 'Importer Factory', desc: 'Routes to FundInfoImporter or NPortImporter' },
                                { step: '3', label: 'Normalize & Validate', desc: 'Column-level validation + numeric coercion' },
                                { step: '4', label: 'Chunked Processing', desc: `CHUNK_SIZE = 50,000 rows per transaction` },
                                { step: '5', label: 'Upsert to Postgres', desc: 'ON CONFLICT DO UPDATE for idempotency' },
                                { step: '6', label: 'Exception Capture', desc: 'Failed rows → ExceptionImports table' },
                                ].map(item => (
                                <div key={item.step} className="flex items-start gap-2.5">
                                    <span className="w-5 h-5 rounded-full bg-primary/20 text-primary text-[10px] font-bold flex items-center justify-center shrink-0 mt-0.5">
                                    {item.step}
                                    </span>
                                    <div>
                                    <p className="font-medium text-foreground" style={{ fontFamily: 'var(--font-display)' }}>{item.label}</p>
                                    <p className="text-[11px] text-muted-foreground">{item.desc}</p>
                                    </div>
                                </div>
                                ))}
                            </div>
                        </div>
                        <div
                        className="hidden lg:block bg-cover bg-center min-h-48"
                        style={{ backgroundImage: `url()` }}
                        />
                    </div>
                </div>

                {/* Upload Zone */}
                <div>
                    <Card className="card-glow">
                        <CardHeader className="pb-3 pt-4 px-5">
                            <CardTitle className="text-sm font-semibold flex items-center gap-2" style={{ fontFamily: 'var(--font-display)' }}>
                            <UploadIcon className="w-4 h-4 text-primary" />
                            File Import
                            </CardTitle>
                        </CardHeader>
                        <CardContent className="px-5 pb-5 space-y-4">
                            {/* Drop zone */}
                            <div
                            className={`border-2 border-dashed rounded-xl p-8 text-center transition-all duration-200 cursor-pointer ${
                                isDragging
                                ? 'border-primary bg-primary/10'
                                : 'border-border hover:border-primary/50 hover:bg-accent/50'
                            }`}
                            onDragOver={e => { e.preventDefault(); setIsDragging(true); }}
                            onDragLeave={() => setIsDragging(false)}
                            onDrop={handleDrop}
                            onClick={() => fileInputRef.current?.click()}
                            >
                            <input
                                ref={fileInputRef}
                                type="file"
                                multiple
                                accept=".tsv,.txt,.csv"
                                className="hidden"
                                onChange={e => addFiles(e.target.files)}
                            />
                            <div className="flex flex-col items-center gap-3">
                                <div className={`p-4 rounded-full transition-colors ${isDragging ? 'bg-primary/20' : 'bg-accent'}`}>

                                <UploadIcon className={`w-6 h-6 transition-colors ${isDragging ? 'text-primary' : 'text-muted-foreground'}`} />

                                </div>
                                <div>
                                <p className="text-sm font-semibold text-foreground" style={{ fontFamily: 'var(--font-display)' }}>
                                    {isDragging ? 'Drop files here' : 'Drop TSV files or click to browse'}
                                </p>
                                <p className="text-xs text-muted-foreground mt-1">
                                    Supports <span className="font-data">fund_info</span> and <span className="font-data">n_port</span> tab-delimited extracts
                                </p>
                                </div>
                            </div>
                            </div>

                            {/* File list */}
                            {files.length > 0 && (
                            <div className="space-y-2">
                                {files.map(uf => (
                                <div key={uf.id} className="bg-accent/50 rounded-lg p-3 space-y-2">
                                    <div className="flex items-center gap-3">
                                    <div className="w-4 h-4 text-muted-foreground shrink-0" />
                                    <div className="flex-1 min-w-0">
                                        <p className="text-xs font-medium text-foreground truncate" style={{ fontFamily: 'var(--font-display)' }}>
                                        {uf.file.name}
                                        </p>
                                        <p className="text-[10px] text-muted-foreground">
                                        {(uf.file.size / 1024).toFixed(1)} KB
                                        </p>
                                    </div>

                                    {/* Type selector */}
                                    {uf.status === 'pending' && (
                                        <Select
                                        value={uf.type}
                                        onValueChange={val => setFiles(prev => prev.map(f => f.id === uf.id ? { ...f, type: val as any } : f))}
                                        >
                                        <SelectTrigger className="h-7 w-32 text-[11px] bg-background border-border">
                                            <SelectValue placeholder="Select type" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="fund_info" className="text-xs">Fund Info</SelectItem>
                                            <SelectItem value="n_port" className="text-xs">N-PORT</SelectItem>
                                        </SelectContent>
                                        </Select>
                                    )}

                                    {/* Status indicator */}
                                    {uf.status === 'done' && <CheckCircle2 className="w-4 h-4 text-emerald-400 shrink-0" />}
                                    {uf.status === 'error' && <AlertTriangle className="w-4 h-4 text-red-400 shrink-0" />}

                                    {/* Actions */}
                                    {uf.status === 'pending' && (
                                        <Button
                                        size="sm"
                                        className="h-7 text-[11px] bg-primary hover:bg-primary/90 gap-1"
                                        disabled={!uf.type}
                                        onClick={() => uploadFile(uf.id)}
                                        >
                                        Import
                                        </Button>
                                    )}
                                    {uf.status === 'pending' && (
                                        <Button
                                        variant="ghost"
                                        size="icon"
                                        className="h-7 w-7 text-muted-foreground hover:text-red-400"
                                        onClick={() => removeFile(uf.id)}
                                        >
                                        <X className="w-3.5 h-3.5" />
                                        </Button>
                                    )}
                                    </div>

                                    {/* Progress bar */}
                                    {(uf.status === 'validating' || uf.status === 'uploading' || uf.status === 'processing') && (
                                    <div className="space-y-1">
                                        {/* <Progress value={uf.progress} className="h-1.5" /> */}
                                        <p className="text-[10px] text-muted-foreground capitalize">
                                        {uf.status}... {uf.progress.toFixed(0)}%
                                        </p>
                                    </div>
                                    )}

                                    {/* Done summary */}
                                    {uf.status === 'done' && (
                                    <div className="flex items-center gap-4 text-[11px]">
                                        <span className="text-emerald-400">
                                        ✓ {uf.rowsProcessed?.toLocaleString()} rows processed
                                        </span>
                                        {uf.exceptionsFound! > 0 && (
                                        <span className="text-amber-400">
                                            ⚠ {uf.exceptionsFound} exceptions flagged
                                        </span>
                                        )}
                                    </div>
                                    )}

                                    {/* Error */}
                                    {uf.status === 'error' && (
                                    <p className="text-[11px] text-red-400">{uf.errorMessage}</p>
                                    )}
                                </div>
                                ))}

                                {/* Bulk action */}
                                {files.filter(f => f.status === 'pending').length > 1 && (
                                <Button
                                    className="w-full h-8 text-xs bg-primary hover:bg-primary/90 gap-2"
                                    disabled={!allReady}
                                    onClick={startAll}
                                >
                                    <UploadIcon className="w-3.5 h-3.5" />
                                    Import All ({files.filter(f => f.status === 'pending').length} files)
                                </Button>
                                )}
                            </div>
                            )}

                            {/* Info box */}
                            <div className="flex items-start gap-2.5 bg-blue-500/10 border border-blue-500/20 rounded-lg p-3">
                            <Info className="w-4 h-4 text-blue-400 shrink-0 mt-0.5" />
                            <div className="text-[11px] text-blue-400/80 space-y-1">
                                <p><strong className="text-blue-300">fund_info</strong> — Tab-delimited with columns: ACCESSION_NUMBER, SERIES_ID, SERIES_LEI, SERIES_NAME, TOTAL_ASSETS, TOTAL_LIABILITIES, NET_ASSETS</p>
                                <p><strong className="text-blue-300">N-PORT</strong> — Streamed in 50,000-row chunks. De-duplicated on (filing_id, issuer_name, cusip). Upserted with ON CONFLICT DO UPDATE.</p>
                            </div>
                            </div>
                        </CardContent>
                    </Card>
                </div>
                
                
            </div>
        </DashboardLayout>
    )

}

export default UploadPage;