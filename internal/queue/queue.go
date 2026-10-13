package queue

import (
        "context"
        "errors"
        "fmt"
        "strconv"
        "strings"
        "time"

        "github.com/redis/go-redis/v9"

        "github.com/Nerotek01/Conduit/internal/model"
)

const (
        GroupName  = "conduit-workers"
        DLQSuffix  = "dlq"
        MaxStreamLen = 100000
)

type Message struct {
        Stream string
        ID     string
        JobID  string
}

type RedisQueue struct {
        client  *redis.Client
        prefix  string
        streams []string
}

func New(client *redis.Client, prefix string) *RedisQueue {
        streams := make([]string, 0, model.MaxPriority)
        for p := model.MinPriority; p <= model.MaxPriority; p++ {
                streams = append(streams, StreamName(prefix, p))
        }
        return &RedisQueue{client: client, prefix: prefix, streams: streams}
}

func StreamName(prefix string, priority int) string {
        return fmt.Sprintf("%s:jobs:p%d", prefix, priority)
}

func DLQName(prefix string) string {
        return fmt.Sprintf("%s:%s", prefix, DLQSuffix)
}

func (q *RedisQueue) Streams() []string {
        return append([]string(nil), q.streams...)
}

func (q *RedisQueue) DLQ() string {
        return DLQName(q.prefix)
}

func (q *RedisQueue) EnsureGroups(ctx context.Context) error {
        for _, stream := range q.streams {
                if err := q.client.XGroupCreateMkStream(ctx, stream, GroupName, "0").Err(); err != nil {
                        if !isBusyGroup(err) {
                                return fmt.Errorf("create group %s: %w", stream, err)
                        }
                }
        }
        return nil
}

func isBusyGroup(err error) bool {
        return err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP")
}

func (q *RedisQueue) Enqueue(ctx context.Context, jobID string, priority int) error {
        if priority < model.MinPriority || priority > model.MaxPriority {
                return fmt.Errorf("enqueue: priority %d outside [%d,%d] would create an unread stream", priority, model.MinPriority, model.MaxPriority)
        }
        stream := StreamName(q.prefix, priority)
        return q.client.XAdd(ctx, &redis.XAddArgs{
                Stream: stream,
                MaxLen: MaxStreamLen,
                Approx: true,
                Values: map[string]any{"job_id": jobID},
        }).Err()
}

func (q *RedisQueue) Read(ctx context.Context, consumer string, count int64, block time.Duration) ([]Message, error) {
        streams := make([]string, 0, len(q.streams)*2)
        streams = append(streams, q.streams...)
        for range q.streams {
                streams = append(streams, ">")
        }
        res, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
                Group:    GroupName,
                Consumer: consumer,
                Streams:  streams,
                Count:    count,
                Block:    block,
        }).Result()
        if err != nil {
                if errors.Is(err, redis.Nil) {
                        return nil, nil
                }
                return nil, fmt.Errorf("xreadgroup: %w", err)
        }
        msgs := make([]Message, 0, count)
        for _, s := range res {
                for _, e := range s.Messages {
                        jobID, ok := e.Values["job_id"]
                        if !ok {
                                _ = q.Ack(ctx, s.Stream, e.ID)
                                continue
                        }
                        msgs = append(msgs, Message{Stream: s.Stream, ID: e.ID, JobID: str(jobID)})
                }
        }
        return msgs, nil
}

func str(v any) string {
        switch t := v.(type) {
        case string:
                return t
        case []byte:
                return string(t)
        default:
                return fmt.Sprintf("%v", t)
        }
}

func (q *RedisQueue) Ack(ctx context.Context, stream, id string) error {
        return q.client.XAck(ctx, stream, GroupName, id).Err()
}

func (q *RedisQueue) SendToDLQ(ctx context.Context, jobID, reason string, attempts int) error {
        return q.client.XAdd(ctx, &redis.XAddArgs{
                Stream: q.DLQ(),
                MaxLen: MaxStreamLen,
                Approx: true,
                Values: map[string]any{
                        "job_id":   jobID,
                        "reason":   reason,
                        "attempts": strconv.Itoa(attempts),
                        "at":       time.Now().UTC().Format(time.RFC3339Nano),
                },
        }).Err()
}

func (q *RedisQueue) ReclaimPending(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]Message, error) {
        var claimed []Message
        for _, stream := range q.streams {
                cursor := "0-0"
                for {
                        res, next, err := q.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
                                Stream:   stream,
                                Group:    GroupName,
                                Consumer: consumer,
                                MinIdle:  minIdle,
                                Count:    count,
                                Start:    cursor,
                        }).Result()
                        if err != nil {
                                return claimed, fmt.Errorf("xautoclaim %s: %w", stream, err)
                        }
                        for _, e := range res {
                                if jobID, ok := e.Values["job_id"]; ok {
                                        claimed = append(claimed, Message{Stream: stream, ID: e.ID, JobID: str(jobID)})
                                } else {
                                        _ = q.Ack(ctx, stream, e.ID)
                                }
                        }
                        cursor = next
                        if cursor == "0-0" || cursor == "" {
                                break
                        }
                }
        }
        return claimed, nil
}

func (q *RedisQueue) Depth(ctx context.Context) (map[string]int64, error) {
        out := make(map[string]int64, len(q.streams)+1)
        for _, stream := range append(q.streams, q.DLQ()) {
                n, err := q.client.XLen(ctx, stream).Result()
                if err != nil && !errors.Is(err, redis.Nil) {
                        return nil, fmt.Errorf("xlen %s: %w", stream, err)
                }
                out[stream] = n
        }
        return out, nil
}

func (q *RedisQueue) Pending(ctx context.Context) (int64, error) {
        var total int64
        for _, stream := range q.streams {
                n, err := q.client.XPending(ctx, stream, GroupName).Result()
                if err != nil {
                        if strings.HasPrefix(err.Error(), "NOGROUP") {
                                continue
                        }
                        return 0, fmt.Errorf("xpending %s: %w", stream, err)
                }
                total += n.Count
        }
        return total, nil
}

func (q *RedisQueue) Ping(ctx context.Context) error {
        return q.client.Ping(ctx).Err()
}

func (q *RedisQueue) Close() error {
        return q.client.Close()
}

func (q *RedisQueue) Prefix() string {
        return q.prefix
}
