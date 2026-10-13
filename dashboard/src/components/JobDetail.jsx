import { useCallback, useEffect, useRef, useState } from 'react'
import { fetchJSON, openEvents } from '../api.js'
import { StatusBadge, shortId, timeAgo } from './JobsView.jsx'

export default function JobDetail({ job, onClose, tick }) {
  const [current, setCurrent] = useState(job)
  const now = Date.now()

  useEffect(() => {
    setCurrent(job)
  }, [job])

  useEffect(() => {
    const conn = openEvents(
      (evt) => {
        if (evt.job && evt.job.id === job.id) {
          setCurrent(evt.job)
        }
      },
      () => {}
    )
    const refresh = setInterval(() => {
      fetchJSON(`/v1/jobs/${job.id}`)
        .then((res) => setCurrent(res.job))
        .catch(() => {})
    }, 5000)
    return () => {
      conn.close()
      clearInterval(refresh)
    }
  }, [job.id])

  const cancel = useCallback(() => {
    fetchJSON(`/v1/jobs/${current.id}`, { method: 'DELETE' })
      .then((res) => setCurrent(res.job))
      .catch(() => {})
  }, [current.id])

  const rows = [
    ['id', current.id],
    ['type', current.type],
    ['status', current.status],
    ['priority', `p${current.priority}`],
    ['attempts', `${current.attempts} / ${current.max_retries} max`],
    ['idempotency key', current.idempotency_key || '-'],
    ['worker', current.worker_id || '-'],
    ['scheduled at', current.scheduled_at || '-'],
    ['started at', current.started_at || '-'],
    ['finished at', current.finished_at || '-'],
    ['created', current.created_at ? timeAgo(current.created_at, now) : '-']
  ]

  return (
    <aside className="detail panel">
      <div className="panel-head">
        <h2>
          Job <span className="mono dim">{shortId(current.id)}</span>
        </h2>
        <div className="detail-actions">
          {['queued', 'scheduled', 'retrying'].includes(current.status) && (
            <button className="btn danger" onClick={cancel}>
              cancel
            </button>
          )}
          <button className="btn" onClick={onClose}>
            close
          </button>
        </div>
      </div>
      <div className="detail-grid">
        {rows.map(([label, value]) => (
          <div key={label} className="detail-row">
            <span className="detail-label">{label}</span>
            <span className="detail-value mono">{value}</span>
          </div>
        ))}
      </div>
      <div className="detail-section">
        <div className="detail-label">status</div>
        <StatusBadge status={current.status} />
      </div>
      <div className="detail-section">
        <div className="detail-label">payload</div>
        <pre className="code">{prettyJSON(current.payload)}</pre>
      </div>
      {current.result && (
        <div className="detail-section">
          <div className="detail-label">result</div>
          <pre className="code">{prettyJSON(current.result)}</pre>
        </div>
      )}
      {current.last_error && (
        <div className="detail-section">
          <div className="detail-label error-text">last error</div>
          <pre className="code error-text">{current.last_error}</pre>
        </div>
      )}
    </aside>
  )
}

function prettyJSON(value) {
  if (!value) return '{}'
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return String(value)
  }
}
