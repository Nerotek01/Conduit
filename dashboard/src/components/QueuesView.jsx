import { useEffect, useState } from 'react'
import { fetchJSON } from '../api.js'

const STREAM_LABELS = {
  'conduit:jobs:p1': 'priority 1 (critical)',
  'conduit:jobs:p2': 'priority 2 (normal)',
  'conduit:jobs:p3': 'priority 3 (low)',
  'conduit:jobs:p4': 'priority 4 (bulk)',
  'conduit:dlq': 'dead letter queue'
}

export default function QueuesView({ snapshot, tick }) {
  const [snap, setSnap] = useState(snapshot)

  useEffect(() => {
    setSnap(snapshot)
  }, [snapshot])

  useEffect(() => {
    fetchJSON('/v1/queues')
      .then(setSnap)
      .catch(() => {})
  }, [tick])

  if (!snap) {
    return <div className="panel empty-panel">loading queue snapshot...</div>
  }

  const streams = Object.entries(snap.streams || {})
  const maxDepth = Math.max(1, ...streams.map(([, depth]) => depth))
  const statuses = snap.statuses || {}

  return (
    <div className="queues-layout">
      <div className="panel">
        <div className="panel-head">
          <h2>Streams</h2>
          <span className="dim mono">pending messages: {snap.pending ?? 0}</span>
        </div>
        <div className="bars">
          {streams.map(([name, depth]) => (
            <div key={name} className="bar-row">
              <span className="bar-label mono">{name}</span>
              <div className="bar-track">
                <div
                  className={`bar-fill ${name.includes('dlq') ? 'danger' : ''}`}
                  style={{ width: `${Math.max(2, (depth / maxDepth) * 100)}%` }}
                />
              </div>
              <span className="bar-value mono">{depth}</span>
            </div>
          ))}
        </div>
        <div className="legend">
          {Object.entries(STREAM_LABELS).map(([key, label]) => (
            <div key={key} className="legend-row">
              <span className="mono dim">{key}</span>
              <span>{label}</span>
            </div>
          ))}
        </div>
      </div>
      <div className="panel">
        <div className="panel-head">
          <h2>Jobs by status</h2>
        </div>
        <table className="table">
          <thead>
            <tr>
              <th>status</th>
              <th>count</th>
            </tr>
          </thead>
          <tbody>
            {['queued', 'scheduled', 'running', 'retrying', 'succeeded', 'dead', 'cancelled'].map(
              (status) => (
                <tr key={status}>
                  <td>
                    <span className="status-badge">{status}</span>
                  </td>
                  <td className="mono">{statuses[status] ?? 0}</td>
                </tr>
              )
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
