package api

import (
        "encoding/json"
        "net/http"
        "sync"
        "time"

        "github.com/gorilla/websocket"

        "github.com/Nerotek01/Conduit/internal/events"
        "github.com/Nerotek01/Conduit/internal/metrics"
)

type wsClient struct {
        conn  *websocket.Conn
        send  chan []byte
}

type Hub struct {
        mu        sync.RWMutex
        clients   map[*wsClient]struct{}
        maxClient int
        register  chan *wsClient
        unregister chan *wsClient
        met       *metrics.Metrics
}

func NewHub(maxClients int, met *metrics.Metrics) *Hub {
        if maxClients < 1 {
                maxClients = 1
        }
        return &Hub{
                clients:    make(map[*wsClient]struct{}),
                maxClient:  maxClients,
                register:   make(chan *wsClient, 16),
                unregister: make(chan *wsClient, 16),
                met:        met,
        }
}

func (h *Hub) Run(bus events.Bus, snapshot func() ([]byte, bool)) {
        go h.listenEvents(bus, snapshot)
        for {
                select {
                case c := <-h.register:
                        h.mu.Lock()
                        h.clients[c] = struct{}{}
                        h.mu.Unlock()
                        h.met.WSclients.Set(float64(len(h.clients)))
                case c := <-h.unregister:
                        h.mu.Lock()
                        if _, ok := h.clients[c]; ok {
                                delete(h.clients, c)
                                close(c.send)
                        }
                        h.mu.Unlock()
                        h.met.WSclients.Set(float64(len(h.clients)))
                }
        }
}

func (h *Hub) listenEvents(bus events.Bus, snapshot func() ([]byte, bool)) {
        if snapshot != nil {
                go func() {
                        ticker := time.NewTicker(2 * time.Second)
                        defer ticker.Stop()
                        for range ticker.C {
                                if body, ok := snapshot(); ok {
                                        h.broadcast(body)
                                }
                        }
                }()
        }
        if ch, ok := bus.(interface{ C() <-chan events.Event }); ok {
                for evt := range ch.C() {
                        body, err := json.Marshal(evt)
                        if err != nil {
                                continue
                        }
                        h.broadcast(body)
                }
        }
}

func (h *Hub) broadcast(body []byte) {
        h.mu.RLock()
        clients := make([]*wsClient, 0, len(h.clients))
        for c := range h.clients {
                clients = append(clients, c)
        }
        h.mu.RUnlock()
        for _, c := range clients {
                select {
                case c.send <- body:
                default:
                        h.disconnect(c)
                }
        }
}

func (h *Hub) disconnect(c *wsClient) {
        h.mu.Lock()
        if _, ok := h.clients[c]; ok {
                delete(h.clients, c)
                close(c.send)
        }
        h.mu.Unlock()
        h.met.WSclients.Set(float64(len(h.clients)))
}

var upgrader = websocket.Upgrader{
        ReadBufferSize:  1024,
        WriteBufferSize: 4096,
}

func (h *Hub) HandleWS(w http.ResponseWriter, r *http.Request) {
        conn, err := upgrader.Upgrade(w, r, nil)
        if err != nil {
                return
        }
        h.mu.RLock()
        count := len(h.clients)
        h.mu.RUnlock()
        if count >= h.maxClient {
                _ = conn.WriteControl(websocket.CloseMessage,
                        websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "client limit reached"),
                        time.Now().Add(2*time.Second))
                _ = conn.Close()
                return
        }
        c := &wsClient{conn: conn, send: make(chan []byte, 64)}
        h.register <- c
        go h.writePump(c)
        h.readPump(c)
}

func (h *Hub) writePump(c *wsClient) {
        ticker := time.NewTicker(20 * time.Second)
        defer ticker.Stop()
        for {
                select {
                case body, ok := <-c.send:
                        _ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
                        if !ok {
                                _ = c.conn.WriteMessage(websocket.CloseMessage, nil)
                                return
                        }
                        if err := c.conn.WriteMessage(websocket.TextMessage, body); err != nil {
                                return
                        }
                case <-ticker.C:
                        _ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
                        if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
                                return
                        }
                }
        }
}

func (h *Hub) readPump(c *wsClient) {
        defer func() {
                h.unregister <- c
                _ = c.conn.Close()
        }()
        c.conn.SetReadLimit(512)
        _ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
        c.conn.SetPongHandler(func(string) error {
                return c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
        })
        for {
                if _, _, err := c.conn.ReadMessage(); err != nil {
                        return
                }
        }
}
