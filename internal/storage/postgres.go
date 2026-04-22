package storage

import (
	"database/sql"

	_ "github.com/lib/pq"

	"distributed-chat-system/internal/logger"
	"go.uber.org/zap"
)

type Store struct {
	DB *sql.DB
}

func NewStore() *Store {
	connStr := "postgres://postgres:postgres123@localhost:5432/chat?sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		logger.Log.Fatal("db connection failed", zap.Error(err))
	}

	return &Store{DB: db}
}

func (s *Store) SaveMessage(sender, receiver, body, conversationID, ClientMsgID string, seq int64) int {
	query := `
	INSERT INTO messages (sender,receiver,body,conversation_id,sequence_number,client_msg_id,delivered)
	VALUES ($1,$2,$3,$4,$5,$6,false)
	ON CONFLICT (client_msg_id) DO NOTHING
	RETURNING id
	`

	var id int
	err := s.DB.QueryRow(query, sender, receiver, body, conversationID, seq, ClientMsgID).Scan(&id)

	if err != nil {
		// duplicate or error
		logger.Log.Warn("insert conflict or failed, fetching existing",
			zap.String("client_msg_id", ClientMsgID),
			zap.Error(err),
		)

		query2 := `SELECT id FROM messages WHERE client_msg_id=$1`
		err2 := s.DB.QueryRow(query2, ClientMsgID).Scan(&id)
		if err2 != nil {
			logger.Log.Error("failed to fetch existing message",
				zap.String("client_msg_id", ClientMsgID),
				zap.Error(err2),
			)
			return 0
		}
	}

	return id
}

type DBMessage struct {
	ID             int
	Sender         string
	Receiver       string
	Body           string
	ConversationID string
	SequenceNumber int64
}

func (s *Store) GetUndelivered(user string) []DBMessage {
	rows, err := s.DB.Query(
		"SELECT id,sender,receiver,body,conversation_id,sequence_number FROM messages WHERE receiver=$1 AND delivered=false ORDER BY conversation_id,sequence_number ASC",
		user,
	)

	if err != nil {
		logger.Log.Error("failed to fetch undelivered messages",
			zap.String("user", user),
			zap.Error(err),
		)
		return nil
	}

	defer rows.Close()

	var result []DBMessage

	for rows.Next() {
		var m DBMessage
		err := rows.Scan(&m.ID, &m.Sender, &m.Receiver, &m.Body, &m.ConversationID, &m.SequenceNumber)
		if err != nil {
			logger.Log.Error("row scan failed",
				zap.Error(err),
			)
			continue
		}
		result = append(result, m)
	}

	return result
}

func (s *Store) MarkDelivered(id int) {
	_, err := s.DB.Exec(
		"UPDATE messages SET delivered=true WHERE id=$1",
		id,
	)

	if err != nil {
		logger.Log.Error("failed to mark delivered",
			zap.Int("message_id", id),
			zap.Error(err),
		)
	}
}

func (s *Store) GetConversationMembers(conversationID string) []string {
	query := `SELECT user_id FROM conversation_members WHERE conversation_id=$1`
	rows, err := s.DB.Query(query, conversationID)
	if err != nil {
		return nil
	}

	defer rows.Close()

	var users []string

	for rows.Next() {
		var u string
		rows.Scan(&u)
		users = append(users, u)
	}
	return users
}

func (s *Store) CreateConversation(conversationID string, members []string) error {
	tx,err := s.DB.Begin()
	if err != nil {
		return err
	}

	// insert conversation
	_,err = tx.Exec(
		"INSERT INTO conversations (id) VALUES ($1)",
		conversationID,
	)
	if err!=nil {
		tx.Rollback()
		return err
	}

	// insert members
	for _, user := range members {
		_, err := tx.Exec(
			"INSERT INTO conversation_members (conversation_id, user_id) VALUES ($1, $2)",
			conversationID,
			user,
		)
		if err!= nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) AddMember(conversationID, userID string) error {
	_,err := s.DB.Exec(
		"INSERT INTO conversation_members (conversation_id, user_id) VALUES ($1, $2)",
		conversationID,
		userID,
	)
	return err
}