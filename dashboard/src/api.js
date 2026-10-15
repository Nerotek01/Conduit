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

let shared = null
let refCount = 0

function createShared() {
  const state = { ws: null, closed: false, retryDelay: 1000 }
  const eventListeners = new Set()
  const statusListeners = new Set()

  function connect() {
    if (state.closed) return
    onStatus('connecting')
    const proto = window.location.protocol === 'https:' ? 'wss' : 'ws'
    state.ws = new WebSocket(`${proto}://${window.location.host}/v1/ws`)
    state.ws.onopen = () => {
      state.retryDelay = 1000
      onStatus('live')
    }
    state.ws.onmessage = (msg) => {
      let evt
      try {
        evt = JSON.parse(msg.data)
      } catch {
        return
      }
      eventListeners.forEach((fn) => {
        try {
          fn(evt)
        } catch {
        }
      })
    }
    state.ws.onclose = () => {
      onStatus('polling')
      if (!state.closed) {
        setTimeout(connect, state.retryDelay)
        state.retryDelay = Math.min(state.retryDelay * 2, 15000)
      }
    }
    state.ws.onerror = () => state.ws.close()
  }

  function onStatus(next) {
    statusListeners.forEach((fn) => {
      try {
        fn(next)
      } catch {
      }
    })
  }

  connect()

  return {
    addEventListener(fn) {
      eventListeners.add(fn)
    },
    removeEventListener(fn) {
      eventListeners.delete(fn)
    },
    addStatusListener(fn) {
      statusListeners.add(fn)
    },
    removeStatusListener(fn) {
      statusListeners.delete(fn)
    },
    destroy() {
      state.closed = true
      if (state.ws) state.ws.close()
    }
  }
}

export function openEvents(onEvent, onStatus) {
  refCount++
  if (!shared) shared = createShared()
  shared.addEventListener(onEvent)
  if (onStatus) shared.addStatusListener(onStatus)
  return {
    close() {
      refCount--
      shared.removeEventListener(onEvent)
      if (onStatus) shared.removeStatusListener(onStatus)
      if (refCount <= 0 && shared) {
        shared.destroy()
        shared = null
        refCount = 0
      }
    }
  }
}
