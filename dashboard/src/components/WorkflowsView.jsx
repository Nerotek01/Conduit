import { useCallback, useEffect, useState } from 'react'
import { fetchJSON } from '../api.js'
import DagView from './DagView.jsx'
import { timeAgo } from './JobsView.jsx'

export default function WorkflowsView({ tick }) {
  const [workflows, setWorkflows] = useState([])
  const [selectedWf, setSelectedWf] = useState(null)
  const [runs, setRuns] = useState([])
  const [selectedRun, setSelectedRun] = useState(null)
  const [newName, setNewName] = useState('')
  const [newDef, setNewDef] = useState('')
  const [error, setError] = useState(null)
  const now = Date.now()

  const loadWorkflows = useCallback(() => {
    fetchJSON('/v1/workflows?limit=100')
      .then((res) => setWorkflows(res.workflows || []))
      .catch(() => {})
  }, [])

  useEffect(() => {
    loadWorkflows()
  }, [loadWorkflows])

  useEffect(() => {
    if (selectedWf) {
      fetchJSON(`/v1/workflows/${selectedWf.id}/runs`)
        .then((res) => setRuns(res.runs || []))
        .catch(() => setRuns([]))
    }
  }, [selectedWf, tick])

  const createWorkflow = () => {
    setError(null)
    let definition
    try {
      definition = JSON.parse(newDef)
    } catch {
      setError('definition is not valid json')
      return
    }
    definition.name = newName || definition.name
    fetchJSON('/v1/workflows', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(definition)
    })
      .then((res) => {
        setWorkflows((prev) => [res.workflow, ...prev])
        setNewName('')
        setNewDef('')
      })
      .catch((err) => setError(err.message))
  }

  const runWorkflow = (wf) => {
    fetchJSON(`/v1/workflows/${wf.id}/run`, { method: 'POST' })
      .then((res) => {
        setRuns((prev) => [res.run, ...prev])
        setSelectedRun(res.run)
      })
      .catch((err) => setError(err.message))
  }

  return (
    <div className="workflows-layout">
      <div className="panel">
        <div className="panel-head">
          <h2>Workflows</h2>
        </div>
        <div className="wf-list">
          {workflows.map((wf) => (
            <div key={wf.id} className={`wf-item ${selectedWf && selectedWf.id === wf.id ? 'selected' : ''}`}>
              <div className="wf-item-main" onClick={() => { setSelectedWf(wf); setSelectedRun(null) }}>
                <span className="wf-name">{wf.name}</span>
                <span className="mono dim">{wf.id.slice(0, 8)}</span>
                <span className="mono dim">{(wf.definition.nodes || []).length} nodes</span>
              </div>
              <button className="btn" onClick={() => runWorkflow(wf)}>
                run
              </button>
            </div>
          ))}
          {workflows.length === 0 && <div className="empty">create a workflow below</div>}
        </div>
        <div className="wf-create">
          <input
            className="input"
            placeholder="workflow name"
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
          />
          <textarea
            className="input mono"
            rows={6}
            placeholder='{"nodes":[{"id":"a","job_type":"echo","payload":{}},{"id":"b","job_type":"echo","depends_on":["a"]}]}'
            value={newDef}
            onChange={(e) => setNewDef(e.target.value)}
          />
          <button className="btn primary" onClick={createWorkflow}>
            create workflow
          </button>
          {error && <div className="error-row">{error}</div>}
        </div>
      </div>
      <div className="panel">
        <div className="panel-head">
          <h2>Runs {selectedWf ? `of ${selectedWf.name}` : ''}</h2>
        </div>
        {!selectedWf && <div className="empty">select a workflow</div>}
        {selectedWf && (
          <>
            <table className="table">
              <thead>
                <tr>
                  <th>run</th>
                  <th>status</th>
                  <th>started</th>
                  <th>finished</th>
                </tr>
              </thead>
              <tbody>
                {runs.map((run) => (
                  <tr
                    key={run.id}
                    className={`row ${selectedRun && selectedRun.id === run.id ? 'selected' : ''}`}
                    onClick={() => setSelectedRun(run)}
                  >
                    <td className="mono">{run.id.slice(0, 8)}</td>
                    <td>
                      <span className="status-badge">{run.status}</span>
                    </td>
                    <td className="dim">{timeAgo(run.started_at, now)}</td>
                    <td className="dim">{run.finished_at ? timeAgo(run.finished_at, now) : '-'}</td>
                  </tr>
                ))}
                {runs.length === 0 && (
                  <tr>
                    <td colSpan={4} className="empty">
                      no runs yet
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
            {selectedRun && (
              <div className="dag-wrap">
                <DagView definition={selectedWf.definition} nodeStates={selectedRun.node_states || {}} />
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}
