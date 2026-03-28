package pubsub

import (
	"context"
	"github.com/redis/go-redis/v9"
	"log"
	"strings"
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
	key := "presence:" + userID + ":" + serverID

	err := r.Client.Set(ctx, key, "1", 30*time.Second).Err()
	if err != nil {
		log.Println("add user server error:", err)
	}

	// set TTL (expiry)
	r.Client.Expire(ctx, key, 30*time.Second)
}

func (r *RedisClient) GetUserServers(userID string) []string {
	pattern := "presence:" + userID + ":*"

	keys, err := r.Client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil
	}

	var servers []string
	for _, key := range keys {
		parts := strings.Split(key, ":")
		if len(parts) == 3 {
			servers = append(servers, parts[2])
		}
	}

	return servers
}

func (r *RedisClient) RemoveUserServer(userID, serverID string) {
	key := "presence:" + userID + ":" + serverID

	err := r.Client.Del(ctx, key).Err()

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
