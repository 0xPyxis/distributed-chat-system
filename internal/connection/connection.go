package connection

import "github.com/gorilla/websocket"

type Connection struct {
	UserID string
	Socket *websocket.Conn
	Send chan[] byte
}