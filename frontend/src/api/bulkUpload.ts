import api from './client'

export interface BulkUploadRow {
  row_number: number
  hotel_name: string
  hotel_name_normalized: string
  brand: string | null
  city: string | null
  state: string | null
  country: string
  opening_date: string | null
  room_count: number | null
  address: string | null
  management_company: string | null
  owner: string | null
  developer: string | null
  hotel_type: string | null
  contact_name: string | null
  contact_email: string | null
  contact_phone: string | null
  notes: string | null
  target_table: string
  dedup?: {
    status: 'new' | 'duplicate_existing' | 'duplicate_lead' | 'duplicate_upload'
    match_id?: number
    match_name?: string
    match_table?: string
    match_status?: string
    similarity?: string
    score?: number
    match_row?: number
  }
}

export interface ParseResponse {
  rows: BulkUploadRow[]
  summary: {
    total: number
    new: number
    duplicate_existing: number
    duplicate_lead: number
    duplicate_upload: number
  }
  column_mapping: Record<string, number>
  headers: string[]
  parse_errors: string[]
}

export interface ImportResponse {
  imported: number
  skipped_duplicates: number
  skipped_errors: number
  hotels: Array<{
    id: number
    name: string
    table: string
    row_number?: number
  }>
  errors: string[]
}

export async function parseUpload(file: File): Promise<ParseResponse> {
  const formData = new FormData()
  formData.append('file', file)
  const { data } = await api.post('/api/bulk-upload/parse', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  })
  return data
}

export async function confirmImport(
  rows: BulkUploadRow[],
  skipDuplicates: boolean = true
): Promise<ImportResponse> {
  const { data } = await api.post('/api/bulk-upload/confirm', {
    rows,
    skip_duplicates: skipDuplicates,
  })
  return data
}
