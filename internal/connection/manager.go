package connection

import (
	"sync"

	"distributed-chat-system/internal/logger"
	"go.uber.org/zap"
)

type Manager struct {
	Connections map[string][]*Connection
	Mutex       sync.RWMutex
}

func NewManager() *Manager {
	return &Manager{
		Connections: make(map[string][]*Connection),
	}
}

func (m *Manager) Add(userID string, conn *Connection) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	m.Connections[userID] = append(m.Connections[userID], conn)

	logger.Log.Info("connection added",
		zap.String("user", userID),
		zap.Int("total_connections", len(m.Connections[userID])),
	)
}

func (m *Manager) Get(userID string) []*Connection {
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()

	return m.Connections[userID]
}

func (m *Manager) Remove(userID string, target *Connection) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	conns, ok := m.Connections[userID]
	if !ok {
		return
	}

	for i, c := range conns {
		if c == target {
			conns = append(conns[:i], conns[i+1:]...)
			break
		}
	}

	if len(conns) == 0 {
		delete(m.Connections, userID)

		logger.Log.Info("all connections removed",
			zap.String("user", userID),
		)
	} else {
		m.Connections[userID] = conns

		logger.Log.Info("connection removed",
			zap.String("user", userID),
			zap.Int("remaining_connections", len(conns)),
		)
	}
}