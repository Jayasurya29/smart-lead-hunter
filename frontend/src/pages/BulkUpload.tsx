import { useState, useCallback, useRef } from 'react'
import { Upload, FileSpreadsheet, CheckCircle2, AlertTriangle, XCircle, Copy, ArrowRight, Building2, Loader2, X, ChevronDown, ChevronUp, Info } from 'lucide-react'
import { parseUpload, confirmImport, type BulkUploadRow, type ParseResponse, type ImportResponse } from '@/api/bulkUpload'

type Step = 'upload' | 'preview' | 'importing' | 'done'

const STATUS_CONFIG = {
  new: {
    label: 'New',
    bg: 'bg-emerald-50',
    text: 'text-emerald-700',
    border: 'border-emerald-200',
    icon: CheckCircle2,
    dot: 'bg-emerald-500',
  },
  duplicate_existing: {
    label: 'Exists',
    bg: 'bg-amber-50',
    text: 'text-amber-700',
    border: 'border-amber-200',
    icon: AlertTriangle,
    dot: 'bg-amber-500',
  },
  duplicate_lead: {
    label: 'In Leads',
    bg: 'bg-orange-50',
    text: 'text-orange-700',
    border: 'border-orange-200',
    icon: Copy,
    dot: 'bg-orange-500',
  },
  duplicate_upload: {
    label: 'Dup in File',
    bg: 'bg-red-50',
    text: 'text-red-700',
    border: 'border-red-200',
    icon: XCircle,
    dot: 'bg-red-400',
  },
} as const

export default function BulkUpload() {
  const [step, setStep] = useState<Step>('upload')
  const [dragActive, setDragActive] = useState(false)
  const [parsing, setParsing] = useState(false)
  const [parseError, setParseError] = useState<string | null>(null)
  const [parseResult, setParseResult] = useState<ParseResponse | null>(null)
  const [selectedRows, setSelectedRows] = useState<Set<number>>(new Set())
  const [importResult, setImportResult] = useState<ImportResponse | null>(null)
  const [showDuplicates, setShowDuplicates] = useState(false)
  const [filterStatus, setFilterStatus] = useState<string>('all')
  const fileInputRef = useRef<HTMLInputElement>(null)

  // ── File handling ──
  const handleFile = useCallback(async (file: File) => {
    const ext = file.name.toLowerCase()
    if (!ext.endsWith('.xlsx') && !ext.endsWith('.xls') && !ext.endsWith('.csv')) {
      setParseError('Please upload an .xlsx, .xls, or .csv file')
      return
    }

    setParsing(true)
    setParseError(null)

    try {
      const result = await parseUpload(file)
      setParseResult(result)

      // Auto-select all "new" rows
      const newRows = new Set<number>()
      result.rows.forEach((row) => {
        if (row.dedup?.status === 'new') {
          newRows.add(row.row_number)
        }
      })
      setSelectedRows(newRows)
      setStep('preview')
    } catch (err: any) {
      setParseError(err?.response?.data?.detail || err.message || 'Failed to parse file')
    } finally {
      setParsing(false)
    }
  }, [])

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault()
      setDragActive(false)
      const file = e.dataTransfer.files?.[0]
      if (file) handleFile(file)
    },
    [handleFile]
  )

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setDragActive(true)
  }, [])

  const handleDragLeave = useCallback(() => setDragActive(false), [])

  const handleInputChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0]
      if (file) handleFile(file)
    },
    [handleFile]
  )

  // ── Row selection ──
  const toggleRow = (rowNum: number) => {
    setSelectedRows((prev) => {
      const next = new Set(prev)
      if (next.has(rowNum)) next.delete(rowNum)
      else next.add(rowNum)
      return next
    })
  }

  const toggleAll = () => {
    if (!parseResult) return
    const filtered = getFilteredRows()
    const allSelected = filtered.every((r) => selectedRows.has(r.row_number))
    if (allSelected) {
      setSelectedRows((prev) => {
        const next = new Set(prev)
        filtered.forEach((r) => next.delete(r.row_number))
        return next
      })
    } else {
      setSelectedRows((prev) => {
        const next = new Set(prev)
        filtered.forEach((r) => next.add(r.row_number))
        return next
      })
    }
  }

  // ── Filtering ──
  const getFilteredRows = useCallback(() => {
    if (!parseResult) return []
    if (filterStatus === 'all') return parseResult.rows
    return parseResult.rows.filter((r) => r.dedup?.status === filterStatus)
  }, [parseResult, filterStatus])

  // ── Import ──
  const handleImport = async () => {
    if (!parseResult) return

    const rowsToImport = parseResult.rows.filter((r) => selectedRows.has(r.row_number))

    if (rowsToImport.length === 0) {
      setParseError('No rows selected for import')
      return
    }

    setStep('importing')
    try {
      const result = await confirmImport(rowsToImport, false)
      setImportResult(result)
      setStep('done')
    } catch (err: any) {
      setParseError(err?.response?.data?.detail || 'Import failed')
      setStep('preview')
    }
  }

  // ── Reset ──
  const reset = () => {
    setStep('upload')
    setParseResult(null)
    setSelectedRows(new Set())
    setImportResult(null)
    setParseError(null)
    setFilterStatus('all')
    if (fileInputRef.current) fileInputRef.current.value = ''
  }

  return (
    <div className="p-6 max-w-[1400px] mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-navy-900 tracking-tight">Bulk Upload</h1>
          <p className="text-stone-500 text-sm mt-1">
            Import a list of hotels from Excel or CSV — duplicates are flagged automatically
          </p>
        </div>
        {step !== 'upload' && (
          <button
            onClick={reset}
            className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-stone-600 bg-stone-100 hover:bg-stone-200 rounded-lg transition-colors"
          >
            <Upload className="w-4 h-4" />
            Upload New File
          </button>
        )}
      </div>

      {/* Step: Upload */}
      {step === 'upload' && (
        <div
          onDrop={handleDrop}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onClick={() => fileInputRef.current?.click()}
          className={`
            relative cursor-pointer rounded-2xl border-2 border-dashed p-16
            flex flex-col items-center justify-center gap-4 transition-all duration-200
            ${dragActive
              ? 'border-amber-400 bg-amber-50/50'
              : 'border-stone-300 bg-white hover:border-amber-300 hover:bg-amber-50/30'
            }
            ${parsing ? 'pointer-events-none opacity-60' : ''}
          `}
        >
          <input
            ref={fileInputRef}
            type="file"
            accept=".xlsx,.xls,.csv"
            onChange={handleInputChange}
            className="hidden"
          />

          {parsing ? (
            <>
              <Loader2 className="w-12 h-12 text-amber-500 animate-spin" />
              <p className="text-lg font-medium text-navy-800">Parsing file & checking duplicates...</p>
            </>
          ) : (
            <>
              <div className="w-16 h-16 rounded-2xl bg-amber-100 flex items-center justify-center">
                <FileSpreadsheet className="w-8 h-8 text-amber-600" />
              </div>
              <div className="text-center">
                <p className="text-lg font-medium text-navy-800">
                  Drop your Excel or CSV file here
                </p>
                <p className="text-sm text-stone-500 mt-1">
                  .xlsx, .xls, or .csv — we'll auto-detect column mappings
                </p>
              </div>
              <div className="flex items-center gap-2 px-4 py-2 rounded-lg bg-amber-500 text-white text-sm font-medium">
                <Upload className="w-4 h-4" />
                Choose File
              </div>
            </>
          )}
        </div>
      )}

      {/* Error */}
      {parseError && (
        <div className="mt-4 p-4 rounded-xl bg-red-50 border border-red-200 flex items-start gap-3">
          <XCircle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
          <div>
            <p className="text-sm font-medium text-red-800">{parseError}</p>
          </div>
          <button onClick={() => setParseError(null)} className="ml-auto text-red-400 hover:text-red-600">
            <X className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Step: Preview */}
      {step === 'preview' && parseResult && (
        <>
          {/* Summary cards */}
          <div className="grid grid-cols-5 gap-3 mb-6">
            <SummaryCard
              label="Total"
              count={parseResult.summary.total}
              color="stone"
              active={filterStatus === 'all'}
              onClick={() => setFilterStatus('all')}
            />
            <SummaryCard
              label="New"
              count={parseResult.summary.new}
              color="emerald"
              active={filterStatus === 'new'}
              onClick={() => setFilterStatus('new')}
            />
            <SummaryCard
              label="In Existing Hotels"
              count={parseResult.summary.duplicate_existing}
              color="amber"
              active={filterStatus === 'duplicate_existing'}
              onClick={() => setFilterStatus('duplicate_existing')}
            />
            <SummaryCard
              label="In New Hotels"
              count={parseResult.summary.duplicate_lead}
              color="orange"
              active={filterStatus === 'duplicate_lead'}
              onClick={() => setFilterStatus('duplicate_lead')}
            />
            <SummaryCard
              label="Dup in File"
              count={parseResult.summary.duplicate_upload}
              color="red"
              active={filterStatus === 'duplicate_upload'}
              onClick={() => setFilterStatus('duplicate_upload')}
            />
          </div>

          {/* Parse errors */}
          {parseResult.parse_errors.length > 0 && (
            <div className="mb-4 p-3 rounded-xl bg-amber-50 border border-amber-200">
              <button
                onClick={() => setShowDuplicates(!showDuplicates)}
                className="flex items-center gap-2 text-sm font-medium text-amber-800 w-full"
              >
                <Info className="w-4 h-4" />
                {parseResult.parse_errors.length} parsing warnings
                {showDuplicates ? <ChevronUp className="w-4 h-4 ml-auto" /> : <ChevronDown className="w-4 h-4 ml-auto" />}
              </button>
              {showDuplicates && (
                <ul className="mt-2 space-y-1">
                  {parseResult.parse_errors.map((e, i) => (
                    <li key={i} className="text-xs text-amber-700">{e}</li>
                  ))}
                </ul>
              )}
            </div>
          )}

          {/* Column mapping info */}
          <div className="mb-4 p-3 rounded-xl bg-blue-50 border border-blue-200 flex items-start gap-2">
            <Info className="w-4 h-4 text-blue-500 mt-0.5 flex-shrink-0" />
            <p className="text-xs text-blue-700">
              Detected columns:{' '}
              {Object.entries(parseResult.column_mapping)
                .map(([field, idx]) => `${field} → "${parseResult.headers[idx]}"`)
                .join(', ')}
            </p>
          </div>

          {/* Table */}
          <div className="bg-white rounded-xl border border-stone-200 overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-stone-50 border-b border-stone-200">
                    <th className="px-3 py-3 text-left">
                      <input
                        type="checkbox"
                        checked={getFilteredRows().length > 0 && getFilteredRows().every((r) => selectedRows.has(r.row_number))}
                        onChange={toggleAll}
                        className="rounded border-stone-300"
                      />
                    </th>
                    <th className="px-3 py-3 text-left text-xs font-semibold text-stone-500 uppercase tracking-wider">Status</th>
                    <th className="px-3 py-3 text-left text-xs font-semibold text-stone-500 uppercase tracking-wider">Hotel Name</th>
                    <th className="px-3 py-3 text-left text-xs font-semibold text-stone-500 uppercase tracking-wider">Brand</th>
                    <th className="px-3 py-3 text-left text-xs font-semibold text-stone-500 uppercase tracking-wider">City</th>
                    <th className="px-3 py-3 text-left text-xs font-semibold text-stone-500 uppercase tracking-wider">State</th>
                    <th className="px-3 py-3 text-left text-xs font-semibold text-stone-500 uppercase tracking-wider">Rooms</th>
                    <th className="px-3 py-3 text-left text-xs font-semibold text-stone-500 uppercase tracking-wider">Opening</th>
                    <th className="px-3 py-3 text-left text-xs font-semibold text-stone-500 uppercase tracking-wider">Destination</th>
                    <th className="px-3 py-3 text-left text-xs font-semibold text-stone-500 uppercase tracking-wider">Match Details</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-stone-100">
                  {getFilteredRows().map((row) => {
                    const status = row.dedup?.status || 'new'
                    const config = STATUS_CONFIG[status as keyof typeof STATUS_CONFIG] || STATUS_CONFIG.new
                    const isSelected = selectedRows.has(row.row_number)
                    const Icon = config.icon

                    return (
                      <tr
                        key={row.row_number}
                        className={`${isSelected ? 'bg-blue-50/30' : 'hover:bg-stone-50'} transition-colors`}
                      >
                        <td className="px-3 py-2.5">
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={() => toggleRow(row.row_number)}
                            className="rounded border-stone-300"
                          />
                        </td>
                        <td className="px-3 py-2.5">
                          <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium ${config.bg} ${config.text}`}>
                            <span className={`w-1.5 h-1.5 rounded-full ${config.dot}`} />
                            {config.label}
                          </span>
                        </td>
                        <td className="px-3 py-2.5 font-medium text-navy-800 max-w-[250px] truncate">{row.hotel_name}</td>
                        <td className="px-3 py-2.5 text-stone-600">{row.brand || '—'}</td>
                        <td className="px-3 py-2.5 text-stone-600">{row.city || '—'}</td>
                        <td className="px-3 py-2.5 text-stone-600">{row.state || '—'}</td>
                        <td className="px-3 py-2.5 text-stone-600">{row.room_count || '—'}</td>
                        <td className="px-3 py-2.5 text-stone-600">{row.opening_date || '—'}</td>
                        <td className="px-3 py-2.5">
                          <span className={`text-xs px-2 py-0.5 rounded ${
                            row.target_table === 'existing' || row.target_table === 'auto'
                              ? 'bg-blue-50 text-blue-700'
                              : 'bg-purple-50 text-purple-700'
                          }`}>
                            {row.target_table === 'potential' ? 'New Hotels' : 'Existing'}
                          </span>
                        </td>
                        <td className="px-3 py-2.5 text-xs text-stone-500 max-w-[200px] truncate">
                          {status === 'new' ? (
                            <span className="text-emerald-600">Ready to import</span>
                          ) : status === 'duplicate_upload' ? (
                            <span>Same as row {row.dedup?.match_row}</span>
                          ) : (
                            <span>
                              Matches{' '}
                              <span className="font-medium text-stone-700">
                                {row.dedup?.match_name}
                              </span>
                              {' '}({row.dedup?.match_table === 'existing_hotels' ? 'existing' : 'lead'} #{row.dedup?.match_id}, {row.dedup?.similarity})
                            </span>
                          )}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>

            {getFilteredRows().length === 0 && (
              <div className="p-12 text-center text-stone-400">
                No rows match the selected filter
              </div>
            )}
          </div>

          {/* Actions */}
          <div className="mt-6 flex items-center justify-between">
            <p className="text-sm text-stone-500">
              {selectedRows.size} of {parseResult.rows.length} rows selected for import
            </p>
            <div className="flex gap-3">
              <button
                onClick={reset}
                className="px-4 py-2.5 text-sm font-medium text-stone-600 bg-stone-100 hover:bg-stone-200 rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleImport}
                disabled={selectedRows.size === 0}
                className="flex items-center gap-2 px-6 py-2.5 text-sm font-semibold text-white bg-amber-500 hover:bg-amber-600 disabled:bg-stone-300 disabled:cursor-not-allowed rounded-lg transition-colors"
              >
                <Building2 className="w-4 h-4" />
                Import {selectedRows.size} Hotels
                <ArrowRight className="w-4 h-4" />
              </button>
            </div>
          </div>
        </>
      )}

      {/* Step: Importing */}
      {step === 'importing' && (
        <div className="flex flex-col items-center justify-center py-24 gap-4">
          <Loader2 className="w-12 h-12 text-amber-500 animate-spin" />
          <p className="text-lg font-medium text-navy-800">Importing hotels...</p>
          <p className="text-sm text-stone-500">This may take a moment</p>
        </div>
      )}

      {/* Step: Done */}
      {step === 'done' && importResult && (
        <div className="bg-white rounded-2xl border border-stone-200 p-8">
          <div className="flex items-center gap-4 mb-6">
            <div className="w-14 h-14 rounded-2xl bg-emerald-100 flex items-center justify-center">
              <CheckCircle2 className="w-7 h-7 text-emerald-600" />
            </div>
            <div>
              <h2 className="text-xl font-bold text-navy-900">Import Complete</h2>
              <p className="text-stone-500 text-sm mt-0.5">
                {importResult.imported} hotels imported successfully
              </p>
            </div>
          </div>

          <div className="grid grid-cols-3 gap-4 mb-6">
            <div className="p-4 rounded-xl bg-emerald-50 border border-emerald-200">
              <p className="text-2xl font-bold text-emerald-700">{importResult.imported}</p>
              <p className="text-xs text-emerald-600 mt-1">Imported</p>
            </div>
            <div className="p-4 rounded-xl bg-amber-50 border border-amber-200">
              <p className="text-2xl font-bold text-amber-700">{importResult.skipped_duplicates}</p>
              <p className="text-xs text-amber-600 mt-1">Duplicates Skipped</p>
            </div>
            <div className="p-4 rounded-xl bg-red-50 border border-red-200">
              <p className="text-2xl font-bold text-red-700">{importResult.skipped_errors}</p>
              <p className="text-xs text-red-600 mt-1">Errors</p>
            </div>
          </div>

          {importResult.errors.length > 0 && (
            <div className="mb-6 p-3 rounded-xl bg-red-50 border border-red-200">
              <p className="text-sm font-medium text-red-800 mb-2">Errors:</p>
              <ul className="space-y-1">
                {importResult.errors.map((e, i) => (
                  <li key={i} className="text-xs text-red-700">{e}</li>
                ))}
              </ul>
            </div>
          )}

          {/* Quick summary of imported hotels */}
          {importResult.hotels.length > 0 && (
            <div className="mb-6">
              <p className="text-sm font-medium text-stone-700 mb-2">Imported hotels:</p>
              <div className="max-h-60 overflow-y-auto space-y-1">
                {importResult.hotels.map((h) => (
                  <div key={`${h.table}-${h.id}`} className="flex items-center gap-2 text-sm py-1">
                    <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500 flex-shrink-0" />
                    <span className="text-navy-800">{h.name}</span>
                    <span className="text-xs text-stone-400">
                      → {h.table === 'existing_hotels' ? 'Existing' : 'New Hotels'} #{h.id}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          <div className="flex gap-3">
            <button
              onClick={reset}
              className="flex items-center gap-2 px-4 py-2.5 text-sm font-medium text-stone-600 bg-stone-100 hover:bg-stone-200 rounded-lg transition-colors"
            >
              <Upload className="w-4 h-4" />
              Upload Another File
            </button>
            <a
              href="/existing-hotels"
              className="flex items-center gap-2 px-4 py-2.5 text-sm font-medium text-white bg-navy-800 hover:bg-navy-900 rounded-lg transition-colors"
            >
              <Building2 className="w-4 h-4" />
              View Existing Hotels
            </a>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Summary card component ──
function SummaryCard({
  label,
  count,
  color,
  active,
  onClick,
}: {
  label: string
  count: number
  color: string
  active: boolean
  onClick: () => void
}) {
  const colorMap: Record<string, string> = {
    stone: 'border-stone-200 bg-stone-50',
    emerald: 'border-emerald-200 bg-emerald-50',
    amber: 'border-amber-200 bg-amber-50',
    orange: 'border-orange-200 bg-orange-50',
    red: 'border-red-200 bg-red-50',
  }
  const textMap: Record<string, string> = {
    stone: 'text-stone-700',
    emerald: 'text-emerald-700',
    amber: 'text-amber-700',
    orange: 'text-orange-700',
    red: 'text-red-700',
  }

  return (
    <button
      onClick={onClick}
      className={`p-3 rounded-xl border transition-all text-left ${
        active
          ? `${colorMap[color]} ring-2 ring-offset-1 ring-${color}-400`
          : `border-stone-200 bg-white hover:${colorMap[color]}`
      }`}
    >
      <p className={`text-2xl font-bold ${textMap[color]}`}>{count}</p>
      <p className="text-xs text-stone-500 mt-0.5">{label}</p>
    </button>
  )
}
