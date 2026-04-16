package pubsub

import (
	"context"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
)

var ctx = context.Background()

type RedisClient struct {
	Client *redis.Client
}

func NewRedis() *RedisClient {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	return &RedisClient{Client: rdb}
}

func (r *RedisClient) Publish(channel string, message []byte) {
	err := r.Client.Publish(ctx, channel, message).Err()
	if err != nil {
		log.Println("publish error:", err)
	}
}

func (r *RedisClient) Subscribe(channel string, handler func([]byte)) {
	pubsub := r.Client.Subscribe(ctx, channel)
	ch := pubsub.Channel()

	go func() {
		for msg := range ch {
			handler([]byte(msg.Payload))

		}
	}()
}

func (r *RedisClient) AddUserServer(userID, serverID string) {
	key := "user_servers:" + userID

	err := r.Client.SAdd(ctx, key, serverID).Err()
	if err != nil {
		log.Println("add user server error:", err)
	}

	// optional TTL (refresh on heartbeat)
	r.Client.Expire(ctx, key, 30*time.Second)
}

func (r *RedisClient) GetUserServers(userID string) []string {
	key := "user_servers:" + userID
	servers, err := r.Client.SMembers(ctx, key).Result()
	if err != nil {
		return nil
	}
	return servers
}

func (r *RedisClient) RemoveUserServer(userID, serverID string) {
	key := "user_servers:" + userID
	err := r.Client.SRem(ctx, key, serverID).Err()
	if err != nil {
		log.Println("remove user server error:", err)
	}
}

func (r *RedisClient) GetNextSequence(conversationID string) (int64, error) {
	key := "conversation:" + conversationID + ":seq"
	seq, err := r.Client.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return seq, nil
}

func (r *RedisClient) AllowMessage(userID string, limit int) bool {
	key := "rate:"+userID

	count,err := r.Client.Incr(ctx,key).Result()
	if err!= nil{
		return false
	}
	if count == 1 {
		r.Client.Expire(ctx, key, time.Second)
	}
	if count>int64(limit) {
		return false
	}
	return true
}