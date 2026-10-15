import { useMemo } from 'react'

const NODE_COLORS = {
  pending: '#8a93a6',
  submitted: '#c7a2ff',
  running: '#54a0ff',
  succeeded: '#3ddc84',
  failed: '#ff5f56',
  skipped: '#5a6070'
}

export default function DagView({ definition, nodeStates }) {
  const layout = useMemo(() => computeLayout(definition), [definition])
  if (!layout) return <div className="empty">no definition</div>

  const { nodes, edges, width, height } = layout

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="dag" preserveAspectRatio="xMidYMid meet">
      <defs>
        <marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto">
          <path d="M0,0 L0,6 L7,3 z" fill="#4a5164" />
        </marker>
      </defs>
      {edges.map(([from, to], i) => {
        const a = nodes[from]
        const b = nodes[to]
        const midX = (a.x + a.w + b.x) / 2
        const path = `M ${a.x + a.w} ${a.y + a.h / 2} C ${midX} ${a.y + a.h / 2}, ${midX} ${b.y + b.h / 2}, ${b.x} ${b.y + b.h / 2}`
        return <path key={i} d={path} className="dag-edge" markerEnd="url(#arrow)" />
      })}
      {Object.entries(nodes).map(([id, node]) => {
        const state = (nodeStates[id] || {}).status || 'pending'
        const color = NODE_COLORS[state] || NODE_COLORS.pending
        return (
          <g key={id} className="dag-node">
            <rect x={node.x} y={node.y} width={node.w} height={node.h} rx="6" className="dag-box" style={{ stroke: color }} />
            <text x={node.x + 10} y={node.y + 18} className="dag-label">
              {id}
            </text>
            <text x={node.x + 10} y={node.y + 34} className="dag-sub" fill={color}>
              {state}
            </text>
            <text x={node.x + 10} y={node.y + 48} className="dag-sub dim">
              {node.jobType}
            </text>
          </g>
        )
      })}
    </svg>
  )
}

function computeLayout(definition) {
  if (!definition || !definition.nodes || definition.nodes.length === 0) return null
  const nodes = definition.nodes
  const deps = {}
  const dependents = {}
  for (const node of nodes) {
    deps[node.id] = node.depends_on || []
    for (const dep of deps[node.id]) {
      dependents[dep] = dependents[dep] || []
      dependents[dep].push(node.id)
    }
  }
  const level = {}
  const queue = nodes.filter((n) => (n.depends_on || []).length === 0).map((n) => n.id)
  for (const id of queue) level[id] = 0
  let visited = queue.length
  const maxSteps = nodes.length * (nodes.length + 2)
  let steps = 0
  while (queue.length > 0) {
    if (steps++ > maxSteps) break
    const cur = queue.shift()
    for (const next of dependents[cur] || []) {
      const nextLevel = level[cur] + 1
      if (level[next] === undefined || nextLevel > level[next]) {
        level[next] = nextLevel
      }
      visited++
      queue.push(next)
    }
  }
  for (const node of nodes) {
    if (level[node.id] === undefined) level[node.id] = 0
  }
  const byLevel = {}
  let maxLevel = 0
  for (const node of nodes) {
    const lvl = level[node.id]
    byLevel[lvl] = byLevel[lvl] || []
    byLevel[lvl].push(node)
    if (lvl > maxLevel) maxLevel = lvl
  }
  const NODE_W = 150
  const NODE_H = 60
  const GAP_X = 70
  const GAP_Y = 26
  const positions = {}
  let maxColumnWidth = 0
  for (let lvl = 0; lvl <= maxLevel; lvl++) {
    const column = byLevel[lvl] || []
    if (column.length > maxColumnWidth) maxColumnWidth = column.length
    column.forEach((node, i) => {
      positions[node.id] = {
        x: lvl * (NODE_W + GAP_X),
        y: i * (NODE_H + GAP_Y),
        w: NODE_W,
        h: NODE_H,
        jobType: node.job_type
      }
    })
  }
  const edges = []
  for (const node of nodes) {
    for (const dep of node.depends_on || []) {
      edges.push([dep, node.id])
    }
  }
  const width = (maxLevel + 1) * (NODE_W + GAP_X) + 20
  const height = maxColumnWidth * (NODE_H + GAP_Y) + 20
  return { nodes: positions, edges, width, height }
}
