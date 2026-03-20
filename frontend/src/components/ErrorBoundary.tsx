import { Component, type ReactNode } from 'react'
import { AlertTriangle, RefreshCw } from 'lucide-react'

interface Props {
  children: ReactNode
  /** Optional: show compact inline error instead of full-page */
  inline?: boolean
  /** Optional: label for logging which panel crashed */
  name?: string
}

interface State {
  hasError: boolean
  error: Error | null
}

export default class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = { hasError: false, error: null }
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, info: React.ErrorInfo) {
    const label = this.props.name || 'ErrorBoundary'
    console.error(`${label} caught:`, error, info.componentStack)
  }

  handleReset = () => {
    this.setState({ hasError: false, error: null })
  }

  handleReload = () => {
    window.location.reload()
  }

  render() {
    if (this.state.hasError) {
      // FIX L-08: Compact inline version for panel/component boundaries
      if (this.props.inline) {
        return (
          <div className="flex flex-col items-center justify-center p-6 text-center">
            <AlertTriangle className="w-5 h-5 text-red-400 mb-2" />
            <p className="text-sm text-stone-500 mb-1">
              {this.props.name ? `${this.props.name} hit an error` : 'Something went wrong'}
            </p>
            {this.state.error && (
              <p className="text-xs text-stone-400 font-mono mb-3 max-w-xs truncate">
                {this.state.error.message}
              </p>
            )}
            <button
              onClick={this.handleReset}
              className="text-xs font-medium text-amber-600 hover:text-amber-700"
            >
              Try Again
            </button>
          </div>
        )
      }

      // Full-page version (app-level boundary)
      return (
        <div className="min-h-full flex items-center justify-center bg-stone-50 p-8">
          <div className="bg-white rounded-xl shadow-lg border border-stone-200 p-8 max-w-md w-full text-center">
            <div className="w-14 h-14 bg-red-100 rounded-xl flex items-center justify-center mx-auto mb-4">
              <AlertTriangle className="w-7 h-7 text-red-500" />
            </div>
            <h2 className="text-lg font-bold text-stone-900 mb-2">Something went wrong</h2>
            <p className="text-sm text-stone-500 mb-1">
              The dashboard hit an unexpected error.
            </p>
            {this.state.error && (
              <p className="text-xs text-stone-400 font-mono bg-stone-50 rounded-lg p-3 mt-3 mb-4 break-all">
                {this.state.error.message}
              </p>
            )}
            <div className="flex items-center justify-center gap-3 mt-5">
              <button
                onClick={this.handleReset}
                className="flex items-center gap-2 px-4 py-2 text-sm font-semibold text-stone-700 bg-stone-100 hover:bg-stone-200 rounded-lg transition"
              >
                Try Again
              </button>
              <button
                onClick={this.handleReload}
                className="flex items-center gap-2 px-4 py-2 text-sm font-semibold text-white bg-navy-800 hover:bg-navy-900 rounded-lg transition"
              >
                <RefreshCw className="w-4 h-4" />
                Reload Page
              </button>
            </div>
          </div>
        </div>
      )
    }

    return this.props.children
  }
}
