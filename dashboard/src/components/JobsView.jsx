import { useCallback, useEffect, useRef, useState } from 'react'
import { fetchJSON, openEvents } from '../api.js'
import JobDetail from './JobDetail.jsx'

const STATUS_FILTERS = ['', 'queued', 'scheduled', 'retrying', 'running', 'succeeded', 'dead', 'cancelled']

const STATUS_COLORS = {
  queued: '#8a93a6',
  scheduled: '#c7a2ff',
  retrying: '#ffb86b',
  running: '#54a0ff',
  succeeded: '#3ddc84',
  dead: '#ff5f56',
  cancelled: '#8a93a6'
}

export function StatusBadge({ status }) {
  return (
    <span className="status-badge" style={{ color: STATUS_COLORS[status] || '#8a93a6' }}>
      {status}
    </span>
  )
}

export function shortId(id) {
  return id ? id.slice(0, 8) : ''
}

export function timeAgo(iso, now) {
  if (!iso) return '-'
  const ms = now - new Date(iso).getTime()
  if (Number.isNaN(ms)) return '-'
  if (ms < 1000) return 'now'
  if (ms < 60000) return `${Math.floor(ms / 1000)}s ago`
  if (ms < 3600000) return `${Math.floor(ms / 60000)}m ago`
  if (ms < 86400000) return `${Math.floor(ms / 3600000)}h ago`
  return `${Math.floor(ms / 86400000)}d ago`
}

export default function JobsView({ tick }) {
  const [jobs, setJobs] = useState([])
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState(null)
  const [error, setError] = useState(null)
  const now = Date.now()
  const pendingRef = useRef(new Map())

  const load = useCallback(() => {
    const path = filter ? `/v1/jobs?limit=100&status=${filter}` : '/v1/jobs?limit=100'
    fetchJSON(path)
      .then((res) => {
        setError(null)
        setJobs(res.jobs || [])
      })
      .catch((err) => setError(err.message))
  }, [filter])

  useEffect(() => {
    load()
  }, [load])

  useEffect(() => {
    const conn = openEvents(
      (evt) => {
        if (evt.event && evt.event.startsWith('job.') && evt.job) {
          pendingRef.current.set(evt.job.id, evt.job)
        }
      },
      () => {}
    )
    const apply = setInterval(() => {
      if (pendingRef.current.size === 0) return
      const updates = pendingRef.current
      pendingRef.current = new Map()
      setJobs((prev) => {
        const map = new Map(prev.map((j) => [j.id, j]))
        for (const [id, job] of updates) {
          if (map.has(id) || !filter || job.status === filter) {
            map.set(id, job)
          }
        }
        const merged = Array.from(map.values())
        merged.sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
        return merged.slice(0, 150)
      })
    }, 400)
    const refresh = setInterval(load, 10000)
    return () => {
      conn.close()
      clearInterval(apply)
      clearInterval(refresh)
    }
  }, [load, filter])

  return (
    <div className="jobs-layout">
      <div className="panel">
        <div className="panel-head">
          <h2>Jobs</h2>
          <div className="filters">
            {STATUS_FILTERS.map((f) => (
              <button
                key={f || 'all'}
                className={`filter ${filter === f ? 'active' : ''}`}
                onClick={() => setFilter(f)}
              >
                {f || 'all'}
              </button>
            ))}
          </div>
        </div>
        {error && <div className="error-row">failed to load jobs: {error}</div>}
        <table className="table">
          <thead>
            <tr>
              <th>id</th>
              <th>type</th>
              <th>status</th>
              <th>prio</th>
              <th>attempts</th>
              <th>worker</th>
              <th>created</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => (
              <tr
                key={job.id}
                className={`row ${selected && selected.id === job.id ? 'selected' : ''}`}
                onClick={() => setSelected(job)}
              >
                <td className="mono">{shortId(job.id)}</td>
                <td>{job.type}</td>
                <td>
                  <StatusBadge status={job.status} />
                </td>
                <td className="mono">p{job.priority}</td>
                <td className="mono">
                  {job.attempts}/{job.max_retries}
                </td>
                <td className="mono">{job.worker_id ? shortId(job.worker_id) : '-'}</td>
                <td className="dim">{timeAgo(job.created_at, now)}</td>
              </tr>
            ))}
            {jobs.length === 0 && (
              <tr>
                <td colSpan={7} className="empty">
                  no jobs {filter ? `with status ${filter}` : 'yet'}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      {selected && <JobDetail job={selected} onClose={() => setSelected(null)} tick={tick} />}
    </div>
  )
}
