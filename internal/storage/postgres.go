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

func (s *Store) SaveMessage(sender, receiver, body string) {
	_, err := s.DB.Exec(
		"INSERT INTO messages (sender, receiver, body, delivered) VALUES ($1, $2, $3, false)",
		sender, receiver, body,
	)

	if err != nil {
		log.Println("save error: ", err)
	}
}

type DBMessage struct {
	ID       int
	Sender   string
	Receiver string
	Body     string
}

func (s *Store) GetUndelivered(user string) []DBMessage {
	rows, err := s.DB.Query(
		"SELECT id,sender,receiver,body FROM messages WHERE receiver=$1 AND delivered=false ORDER BY created_at",
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
