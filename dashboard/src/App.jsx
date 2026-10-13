import { useEffect, useMemo, useRef, useState, useCallback } from 'react'
import { fetchJSON, openEvents } from './api.js'
import JobsView from './components/JobsView.jsx'
import QueuesView from './components/QueuesView.jsx'
import WorkersView from './components/WorkersView.jsx'
import WorkflowsView from './components/WorkflowsView.jsx'

const TABS = [
  { id: 'jobs', label: 'Jobs' },
  { id: 'queues', label: 'Queues' },
  { id: 'workers', label: 'Workers' },
  { id: 'workflows', label: 'Workflows' }
]

export default function App() {
  const [tab, setTab] = useState('jobs')
  const [status, setStatus] = useState('connecting')
  const [health, setHealth] = useState(null)
  const [queueSnapshot, setQueueSnapshot] = useState(null)
  const [tick, setTick] = useState(0)
  const connectionRef = useRef(null)
  // Status lives in a ref so the polling interval can read it without making
  // the effect depend on it. Depending on `status` re-ran the effect on every
  // connection state change, tearing down and reconnecting the WebSocket in
  // an infinite loop (live -> teardown -> polling -> reconnect -> ...).
  const statusRef = useRef('connecting')

  const refreshHealth = useCallback(() => {
    fetchJSON('/healthz')
      .then(setHealth)
      .catch(() => setHealth({ status: 'unreachable' }))
  }, [])

  const refreshQueues = useCallback(() => {
    fetchJSON('/v1/queues')
      .then(setQueueSnapshot)
      .catch(() => {})
  }, [])

  const handleStatus = useCallback((next) => {
    statusRef.current = next
    setStatus(next)
  }, [])

  useEffect(() => {
    refreshHealth()
    refreshQueues()
    connectionRef.current = openEvents(
      (evt) => {
        if (evt.event === 'queue.depth' && evt.data) {
          setQueueSnapshot(evt.data)
        }
      },
      handleStatus
    )
    const poll = setInterval(() => {
      refreshHealth()
      if (statusRef.current !== 'live') refreshQueues()
    }, 5000)
    const bump = setInterval(() => setTick((t) => t + 1), 1000)
    return () => {
      clearInterval(poll)
      clearInterval(bump)
      if (connectionRef.current) connectionRef.current.close()
    }
  }, [refreshHealth, refreshQueues, handleStatus])

  const badgeClass = status === 'live' ? 'badge live' : 'badge polling'

  return (
    <div className="app">
      <header className="header">
        <div className="brand">
          <span className="brand-mark">C</span>
          <div>
            <div className="brand-name">Conduit</div>
            <div className="brand-sub">distributed job queue &amp; workflow engine</div>
          </div>
        </div>
        <div className="header-right">
          {queueSnapshot && (
            <div className="depth-chips">
              {Object.entries(queueSnapshot.statuses || {})
                .filter(([name]) => ['queued', 'running', 'retrying', 'dead'].includes(name))
                .map(([name, count]) => (
                  <span key={name} className={`chip chip-${name}`}>
                    {count} {name}
                  </span>
                ))}
            </div>
          )}
          <span className={badgeClass}>{status === 'live' ? 'live' : 'polling'}</span>
          <span className={`badge ${health && health.status === 'ok' ? 'ok' : 'down'}`}>
            {health ? health.status : '...'}
          </span>
        </div>
      </header>
      <nav className="tabs">
        {TABS.map((t) => (
          <button
            key={t.id}
            className={`tab ${tab === t.id ? 'active' : ''}`}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </nav>
      <main className="content">
        {tab === 'jobs' && <JobsView tick={tick} />}
        {tab === 'queues' && <QueuesView snapshot={queueSnapshot} tick={tick} />}
        {tab === 'workers' && <WorkersView tick={tick} />}
        {tab === 'workflows' && <WorkflowsView tick={tick} />}
      </main>
    </div>
  )
}
