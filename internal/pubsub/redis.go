package pubsub

import (
	"context"
	"github.com/redis/go-redis/v9"
	"log"
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
	err := r.Client.SAdd(ctx, "user:"+userID, serverID).Err()
	if err != nil {
		log.Println("add user server error:", err)
	}
}

func (r *RedisClient) GetUserServers(userID string) []string {
	vals, err := r.Client.SMembers(ctx, "user:"+userID).Result()
	if err != nil {
		return nil
	}
	return vals
}

func (r *RedisClient) RemoveUserServer(userID, serverID string) {
	err := r.Client.SRem(ctx, "user:"+userID, serverID).Err()
	if err != nil {
		log.Println("remove user server error:", err)
	}
}


