import { createContext, useContext, useState, useCallback, useRef, useEffect, ReactNode } from 'react'
import { checkDiscoveryStatus } from '@/api/leads'

/* ── Types ── */

export interface TaskSummary {
  type: 'scrape' | 'extract' | 'discovery'
  message: string
  newLeads: number
  duration: number
}

interface BackgroundTaskState {
  isRunning: boolean
  taskType: string | null
  eventCount: number
  status: 'idle' | 'running' | 'done' | 'error'
  summary: TaskSummary | null
  logs: string[]
  startTask: (type: string) => void
  addEvent: () => void
  addLog: (msg: string) => void
  completeTask: (summary: TaskSummary) => void
  failTask: (errorMsg: string) => void
  dismissToast: () => void
  startBackgroundPoll: (discoveryId: string) => void
}

const BackgroundTaskContext = createContext<BackgroundTaskState | null>(null)

/* ── Provider ── */

export function BackgroundTaskProvider({ children }: { children: ReactNode }) {
  const [isRunning, setIsRunning] = useState(false)
  const [taskType, setTaskType] = useState<string | null>(null)
  const [eventCount, setEventCount] = useState(0)
  const [status, setStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle')
  const [summary, setSummary] = useState<TaskSummary | null>(null)
  const [logs, setLogs] = useState<string[]>([])
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Clean up polling on unmount
  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
    }
  }, [])

  const startTask = useCallback((type: string) => {
    setIsRunning(true)
    setTaskType(type)
    setEventCount(0)
    setStatus('running')
    setSummary(null)
    setLogs([])
  }, [])

  const addEvent = useCallback(() => {
    setEventCount((prev) => prev + 1)
  }, [])

  const addLog = useCallback((msg: string) => {
    setLogs((prev) => [...prev, msg])
    setEventCount((prev) => prev + 1)
  }, [])

  const completeTask = useCallback((s: TaskSummary) => {
    setIsRunning(false)
    setStatus('done')
    setSummary(s)
    if (pollRef.current) {
      clearInterval(pollRef.current)
      pollRef.current = null
    }
  }, [])

  const failTask = useCallback((errorMsg: string) => {
    setIsRunning(false)
    setStatus('error')
    setSummary({
      type: (taskType as any) || 'scrape',
      message: errorMsg || 'Task failed',
      newLeads: 0,
      duration: 0,
    })
    if (pollRef.current) {
      clearInterval(pollRef.current)
      pollRef.current = null
    }
  }, [taskType])

  const dismissToast = useCallback(() => {
    setSummary(null)
    setStatus('idle')
    setLogs([])
  }, [])

  const startBackgroundPoll = useCallback((discoveryId: string) => {
    // Clear any existing poll
    if (pollRef.current) clearInterval(pollRef.current)

    pollRef.current = setInterval(async () => {
      try {
        const result = await checkDiscoveryStatus(discoveryId)
        if (result.done) {
          const stats = result.stats || {}
          setIsRunning(false)
          setStatus('done')
          setSummary({
            type: 'discovery',
            message: 'Discovery complete (background)',
            newLeads: stats.leads ?? stats.sources ?? 0,
            duration: 0,
          })
          if (pollRef.current) {
            clearInterval(pollRef.current)
            pollRef.current = null
          }
        }
      } catch {
        // Silently retry on network errors
      }
    }, 3000)
  }, [])

  return (
    <BackgroundTaskContext.Provider
      value={{
        isRunning, taskType, eventCount, status,
        summary, logs,
        startTask, addEvent, addLog, completeTask, failTask, dismissToast,
        startBackgroundPoll,
      }}
    >
      {children}
    </BackgroundTaskContext.Provider>
  )
}

/* ── Hook ── */

export function useBackgroundTask() {
  const ctx = useContext(BackgroundTaskContext)
  if (!ctx) throw new Error('useBackgroundTask must be used within BackgroundTaskProvider')
  return ctx
}
