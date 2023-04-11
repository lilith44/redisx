package redisx

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/lilith44/easy"
)

type Redis struct {
	client *redis.Client

	prefix string
}

func New(c Config) (*Redis, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     c.Address,
		Username: c.Username,
		Password: c.Password,
		DB:       c.DB,
		PoolSize: c.PoolSize,
	})

	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		return nil, err
	}

	return &Redis{client: client, prefix: c.Prefix}, nil
}

func addPrefix(prefix string, key string) string {
	if prefix == "" {
		return key
	}

	return prefix + "_" + key
}

func parseExpiration(expiration []time.Duration) time.Duration {
	if len(expiration) == 0 {
		return 0
	}

	return expiration[0]
}

type value struct {
	val any
}

func newSetValue(val any) any {
	switch val.(type) {
	case nil, string, []byte, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
	case bool, time.Time, time.Duration:
	default:
		return &value{val: val}
	}

	return val
}

func newGetValue(valPtr any) any {
	switch valPtr.(type) {
	case *string, *[]byte, *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64, *float32, *float64:
	case *bool, *time.Time, *time.Duration:
	default:
		return &value{val: valPtr}
	}

	return valPtr
}

func (v *value) MarshalBinary() ([]byte, error) {
	return json.Marshal(v.val)
}

func (v *value) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, v.val)
}

func (r *Redis) Client() *redis.Client {
	return r.client
}

func (r *Redis) UniqueIdGenerator(key string, expiration time.Duration) easy.UniqueIdGenerator {
	return func() int64 {
		var id int64
		for {
			set, err := r.SetNX(context.Background(), fmt.Sprintf("%s_%d", key, id), "", expiration)
			if err != nil {
				panic(err)
			}

			if set {
				break
			}

			id++
		}

		return id
	}
}

func (r *Redis) Set(ctx context.Context, key string, value any, expiration ...time.Duration) (string, error) {
	return r.client.Set(ctx, addPrefix(r.prefix, key), newSetValue(value), parseExpiration(expiration)).Result()
}

func (r *Redis) SetNX(ctx context.Context, key string, value any, expiration ...time.Duration) (bool, error) {
	return r.client.SetNX(ctx, addPrefix(r.prefix, key), newSetValue(value), parseExpiration(expiration)).Result()
}

func (r *Redis) Get(ctx context.Context, key string, valuePtr any) error {
	return r.client.Get(ctx, addPrefix(r.prefix, key)).Scan(newGetValue(valuePtr))
}

func (r *Redis) Del(ctx context.Context, key string) (int64, error) {
	return r.client.Del(ctx, addPrefix(r.prefix, key)).Result()
}

func (r *Redis) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	return r.client.IncrBy(ctx, addPrefix(r.prefix, key), value).Result()
}

func (r *Redis) TTL(ctx context.Context, key string) (time.Duration, error) {
	return r.client.TTL(ctx, addPrefix(r.prefix, key)).Result()
}

func (r *Redis) Expire(ctx context.Context, key string, duration time.Duration) (bool, error) {
	return r.client.Expire(ctx, addPrefix(r.prefix, key), duration).Result()
}

func (r *Redis) HSet(ctx context.Context, key string, mapping map[string]any) (int64, error) {
	for k := range mapping {
		mapping[k] = newSetValue(mapping[k])
	}

	return r.client.HSet(ctx, addPrefix(r.prefix, key), mapping).Result()
}

func (r *Redis) HSetNX(ctx context.Context, key string, field string, value any) (bool, error) {
	return r.client.HSetNX(ctx, addPrefix(r.prefix, key), field, newSetValue(value)).Result()
}

func (r *Redis) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return r.client.HGetAll(ctx, addPrefix(r.prefix, key)).Result()
}

func (r *Redis) HGet(ctx context.Context, key string, field string, valuePtr any) error {
	return r.client.HGet(ctx, addPrefix(r.prefix, key), field).Scan(newGetValue(valuePtr))
}

func (r *Redis) HLen(ctx context.Context, key string) (int64, error) {
	return r.client.HLen(ctx, addPrefix(r.prefix, key)).Result()
}

func (r *Redis) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	return r.client.HDel(ctx, addPrefix(r.prefix, key), fields...).Result()
}

func (r *Redis) ZAdd(ctx context.Context, key string, members ...*redis.Z) (int64, error) {
	return r.client.ZAdd(ctx, addPrefix(r.prefix, key), members...).Result()
}

func (r *Redis) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) ([]string, error) {
	return r.client.ZRangeByScore(ctx, addPrefix(r.prefix, key), opt).Result()
}

func (r *Redis) ZRemRangeByScore(ctx context.Context, key string, min, max string) (int64, error) {
	return r.client.ZRemRangeByScore(ctx, addPrefix(r.prefix, key), min, max).Result()
}

func (r *Redis) ZScore(ctx context.Context, key string, member string) (float64, error) {
	return r.client.ZScore(ctx, addPrefix(r.prefix, key), member).Result()
}

func (r *Redis) ZCard(ctx context.Context, key string) (int64, error) {
	return r.client.ZCard(ctx, addPrefix(r.prefix, key)).Result()
}

func (r *Redis) ZRem(ctx context.Context, key string, members ...string) (int64, error) {
	return r.client.ZRem(ctx, addPrefix(r.prefix, key), easy.ToAnySlice(members)...).Result()
}
