package connection

import "sync"

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
}

func (m *Manager) Get(userID string) []*Connection {
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()

	return m.Connections[userID]
}

func (m *Manager) Remove(userID string, target *Connection) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	conns := m.Connections[userID]

	for i, c := range conns {
		if c == target {
			conns = append(conns[:i], conns[i+1:]...)
			break
		}
	}

	if len(conns) == 0 {
		delete(m.Connections, userID)
	} else {
		m.Connections[userID] = conns
	}
}
