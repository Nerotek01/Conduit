const BASE = ''

export async function fetchJSON(path, options) {
  const resp = await fetch(BASE + path, options)
  if (!resp.ok) {
    let message = `HTTP ${resp.status}`
    try {
      const body = await resp.json()
      if (body.error) message = body.error
    } catch {
    }
    throw new Error(message)
  }
  if (resp.status === 204) return null
  return resp.json()
}

export function openEvents(onEvent, onStatus) {
  const proto = window.location.protocol === 'https:' ? 'wss' : 'ws'
  let ws
  let closed = false
  let retryDelay = 1000

  function connect() {
    if (closed) return
    onStatus('connecting')
    ws = new WebSocket(`${proto}://${window.location.host}/v1/ws`)
    ws.onopen = () => {
      retryDelay = 1000
      onStatus('live')
    }
    ws.onmessage = (msg) => {
      try {
        onEvent(JSON.parse(msg.data))
      } catch {
      }
    }
    ws.onclose = () => {
      onStatus('polling')
      if (!closed) {
        setTimeout(connect, retryDelay)
        retryDelay = Math.min(retryDelay * 2, 15000)
      }
    }
    ws.onerror = () => ws.close()
  }

  connect()

  return {
    close() {
      closed = true
      if (ws) ws.close()
    }
  }
}
