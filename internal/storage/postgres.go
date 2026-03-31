package storage

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

type Store struct {
	DB *sql.DB
}

func NewStore() *Store {
	connStr := "postgres://postgres:postgres123@localhost:5432/chat?sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
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
		// duplicate -> fetching existing id
		query2 := `SELECT id FROM messages WHERE client_msg_id=$1`
		s.DB.QueryRow(query2,ClientMsgID).Scan(&id)
	}
	return id
}

type DBMessage struct {
	ID       int
	Sender   string
	Receiver string
	Body     string
	ConversationID string
	SequenceNumber int64
}

func (s *Store) GetUndelivered(user string) []DBMessage {
	rows, err := s.DB.Query(
		"SELECT id,sender,receiver,body,conversation_id,sequence_number FROM messages WHERE receiver=$1 AND delivered=false ORDER BY conversation_id,sequence_number ASC",
		user,
	)

	if err != nil {
		log.Println(err)
		return nil
	}

	defer rows.Close()

	var result []DBMessage

	for rows.Next() {
		var m DBMessage
		rows.Scan(&m.ID, &m.Sender, &m.Receiver, &m.Body)
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
		log.Println(err)
	}
}
