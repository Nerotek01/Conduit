import { useEffect, useState } from 'react'
import { fetchJSON } from '../api.js'
import { timeAgo } from './JobsView.jsx'

export default function WorkersView({ tick }) {
  const [workers, setWorkers] = useState([])
  const now = Date.now()

  useEffect(() => {
    const refresh = setInterval(() => {
      fetchJSON('/v1/workers')
        .then((res) => setWorkers(res.workers || []))
        .catch(() => {})
    }, 5000)
    fetchJSON('/v1/workers')
      .then((res) => setWorkers(res.workers || []))
      .catch(() => {})
    return () => clearInterval(refresh)
  }, [])

  return (
    <div className="panel">
      <div className="panel-head">
        <h2>Workers</h2>
        <span className="dim mono">
          {workers.filter((w) => w.status === 'active').length} active / {workers.length} total
        </span>
      </div>
      <table className="table">
        <thead>
          <tr>
            <th>id</th>
            <th>hostname</th>
            <th>status</th>
            <th>concurrency</th>
            <th>heartbeat</th>
            <th>started</th>
          </tr>
        </thead>
        <tbody>
          {workers.map((w) => {
            const stale = now - new Date(w.last_heartbeat).getTime() > 15000
            return (
              <tr key={w.id} className={w.status === 'dead' ? 'row-dead' : ''}>
                <td className="mono">{w.id}</td>
                <td className="mono">{w.hostname}</td>
                <td>
                  <span className={`status-badge ${stale ? 'dim' : ''}`} style={{ color: w.status === 'active' && !stale ? '#3ddc84' : '#ff5f56' }}>
                    {w.status === 'active' && stale ? 'stale' : w.status}
                  </span>
                </td>
                <td className="mono">{w.concurrency}</td>
                <td className="dim">{timeAgo(w.last_heartbeat, now)}</td>
                <td className="dim">{timeAgo(w.started_at, now)}</td>
              </tr>
            )
          })}
          {workers.length === 0 && (
            <tr>
              <td colSpan={6} className="empty">
                no workers registered
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}
