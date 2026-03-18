import React, { createContext, useContext, useState, useRef, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { createSSEStream } from '@/api/leads'
import type { SSEEvent } from '@/api/types'

// ── Types ──

type TaskType = 'scrape' | 'extract' | 'discovery'
type TaskStatus = 'idle' | 'running' | 'done' | 'error'

interface TaskSummary {
  type: TaskType
  leadsFound: number
  newLeads: number
  duration: number          // seconds
  message: string           // final status message
}

interface BackgroundTaskState {
  status: TaskStatus
  taskType: TaskType | null
  eventCount: number
  logs: string[]
  summary: TaskSummary | null
  // Actions
  startTask: (type: TaskType, streamUrl: string) => void
  dismissToast: () => void
  isRunning: boolean
}

// ── Context ──

const BackgroundTaskContext = createContext<BackgroundTaskState | null>(null)

export function useBackgroundTask() {
  const ctx = useContext(BackgroundTaskContext)
  if (!ctx) throw new Error('useBackgroundTask must be used within BackgroundTaskProvider')
  return ctx
}

// ── Provider ──

export function BackgroundTaskProvider({ children }: { children: React.ReactNode }) {
  const [status, setStatus] = useState<TaskStatus>('idle')
  const [taskType, setTaskType] = useState<TaskType | null>(null)
  const [eventCount, setEventCount] = useState(0)
  const [logs, setLogs] = useState<string[]>([])
  const [summary, setSummary] = useState<TaskSummary | null>(null)

  const esRef = useRef<EventSource | null>(null)
  const qc = useQueryClient()

  const startTask = useCallback((type: TaskType, streamUrl: string) => {
    // Close any existing stream
    esRef.current?.close()

    // Reset state
    setStatus('running')
    setTaskType(type)
    setEventCount(0)
    setLogs([`Starting ${type}...`])
    setSummary(null)

    const label = type === 'scrape' ? 'Scrape' : type === 'extract' ? 'Extraction' : 'Discovery'

    const es = createSSEStream(streamUrl)
    esRef.current = es

    let evtCount = 0
    let lastStats: Record<string, any> = {}

    es.onmessage = (e) => {
      try {
        const d: SSEEvent = JSON.parse(e.data)
        if (d.message) {
          setLogs(p => [...p, d.message])
          evtCount++
          setEventCount(evtCount)
        }

        // Capture stats for summary
        if (d.stats) lastStats = d.stats

        if (d.type === 'complete' || d.type === 'error') {
          const isError = d.type === 'error'
          es.close()
          esRef.current = null

          setStatus(isError ? 'error' : 'done')
          setSummary({
            type,
            leadsFound: lastStats.leads_found ?? lastStats.total_leads ?? 0,
            newLeads: lastStats.leads_new ?? lastStats.new_leads ?? 0,
            duration: d.duration_seconds ?? 0,
            message: isError
              ? `${label} failed`
              : `${label} complete`,
          })

          // Refresh all data
          qc.invalidateQueries({ queryKey: ['leads'] })
          qc.invalidateQueries({ queryKey: ['stats'] })
          qc.invalidateQueries({ queryKey: ['sources'] })
        }
      } catch {
        if (e.data && e.data !== 'ping') {
          setLogs(p => [...p, e.data])
          evtCount++
          setEventCount(evtCount)
        }
      }
    }

    es.onerror = () => {
      es.close()
      esRef.current = null
      setStatus('done')
      setSummary({
        type,
        leadsFound: lastStats.leads_found ?? lastStats.total_leads ?? 0,
        newLeads: lastStats.leads_new ?? lastStats.new_leads ?? 0,
        duration: 0,
        message: `${label} complete`,
      })
      qc.invalidateQueries({ queryKey: ['leads'] })
      qc.invalidateQueries({ queryKey: ['stats'] })
      qc.invalidateQueries({ queryKey: ['sources'] })
    }
  }, [qc])

  const dismissToast = useCallback(() => {
    setSummary(null)
    setStatus('idle')
    setTaskType(null)
    setLogs([])
    setEventCount(0)
  }, [])

  return (
    <BackgroundTaskContext.Provider value={{
      status,
      taskType,
      eventCount,
      logs,
      summary,
      startTask,
      dismissToast,
      isRunning: status === 'running',
    }}>
      {children}
    </BackgroundTaskContext.Provider>
  )
}
