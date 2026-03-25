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

func (r *RedisClient) SetUserServer(userID, serverID string) {
	err := r.Client.Set(ctx, "user:"+userID, serverID, 0).Err()
	if err != nil {
		log.Println("set user server error:", err)
	}
}

func (r *RedisClient) GetUserServer(userID string) string {
	val, err := r.Client.Get(ctx, "user:"+userID).Result()
	if err != nil {
		return ""
	}
	return val
}

func (r *RedisClient) RemoveUser(userID string) {
	err:= r.Client.Del(ctx, "user:"+userID).Err()
	if err!=nil {
		log.Println("remove user error:", err)
	}
}

