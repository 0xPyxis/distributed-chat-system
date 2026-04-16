package pubsub

import (
	"context"
	"time"

	"distributed-chat-system/internal/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
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
		logger.Log.Error("publish failed",
			zap.String("channel", channel),
			zap.Error(err),
		)
	}
}

func (r *RedisClient) Subscribe(channel string, handler func([]byte)) {
	pubsub := r.Client.Subscribe(ctx, channel)
	ch := pubsub.Channel()

	go func() {
		for msg := range ch {
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						logger.Log.Error("panic in subscriber",
							zap.Any("recover", rec),
						)
					}
				}()
				handler([]byte(msg.Payload))
			}()
		}
	}()
}

func (r *RedisClient) AddUserServer(userID, serverID string) {
	key := "user_servers:" + userID

	err := r.Client.SAdd(ctx, key, serverID).Err()
	if err != nil {
		logger.Log.Error("add user server failed",
			zap.String("user", userID),
			zap.String("server", serverID),
			zap.Error(err),
		)
	}

	r.Client.Expire(ctx, key, 30*time.Second)
}

func (r *RedisClient) GetUserServers(userID string) []string {
	key := "user_servers:" + userID

	servers, err := r.Client.SMembers(ctx, key).Result()
	if err != nil {
		logger.Log.Error("get user servers failed",
			zap.String("user", userID),
			zap.Error(err),
		)
		return nil
	}

	return servers
}

func (r *RedisClient) RemoveUserServer(userID, serverID string) {
	key := "user_servers:" + userID

	err := r.Client.SRem(ctx, key, serverID).Err()
	if err != nil {
		logger.Log.Error("remove user server failed",
			zap.String("user", userID),
			zap.String("server", serverID),
			zap.Error(err),
		)
	}
}

func (r *RedisClient) GetNextSequence(conversationID string) (int64, error) {
	key := "conversation:" + conversationID + ":seq"

	seq, err := r.Client.Incr(ctx, key).Result()
	if err != nil {
		logger.Log.Error("sequence increment failed",
			zap.String("conversation", conversationID),
			zap.Error(err),
		)
		return 0, err
	}

	return seq, nil
}

func (r *RedisClient) AllowMessage(userID string, limit int) bool {
	key := "rate:" + userID

	count, err := r.Client.Incr(ctx, key).Result()
	if err != nil {
		logger.Log.Error("rate limit check failed",
			zap.String("user", userID),
			zap.Error(err),
		)
		return false
	}

	if count == 1 {
		r.Client.Expire(ctx, key, time.Second)
	}

	if count > int64(limit) {
		logger.Log.Warn("rate limit exceeded",
			zap.String("user", userID),
			zap.Int64("count", count),
		)
		return false
	}

	return true
}
