package gomb

import (
	"context"
	"strconv"
	"sync"
	"time"

	// external
	"github.com/redis/go-redis/v9"
)

// *******************************************
// Redis Streams Broker
// *******************************************

// redisStreamsBroker is a Redis Streams broker.
type redisStreamsBroker struct {
	// Host is the Redis host, including port.
	Host string

	// Password is the Redis password.
	Password string

	// DB is the Redis database.
	DB int

	// Client is the Redis client.
	Client *redis.Client

	options *BrokerOptions
}

// newRedisStreamsBroker creates a new Redis Streams broker.
func newRedisStreamsBroker(opts BrokerOptions) (*redisStreamsBroker, error) {
	var err error

	// if auth is set, extract the password
	pwrd := ""
	if opts.Auth != nil {
		if opts.Auth.Basic != nil {
			pwrd = opts.Auth.Basic.Password
		}
	}

	// extract the database from the address
	// default to 0
	db := 0
	if opts.Address != "" {
		dbStr := opts.Address[len(opts.Address)-1]
		// check if integer
		db, err = strconv.Atoi(string(dbStr))
		if err != nil {
			db = 0
		} else {
			// remove the database from the address
			// there is a colon before the database
			opts.Address = opts.Address[:len(opts.Address)-2]
		}
	}

	return &redisStreamsBroker{
		Host:     opts.Address,
		Password: pwrd,
		DB:       db,
	}, nil
}

// Connect connects to the Redis Streams broker.
func (b *redisStreamsBroker) Connect(ctx context.Context) error {
	if b.Client != nil {
		return nil
	}

	b.Client = redis.NewClient(&redis.Options{
		Addr:     b.Host,
		Password: b.Password,
		DB:       b.DB,
	})

	return nil
}

// Close closes the Redis Streams broker.
func (b *redisStreamsBroker) Close() error {
	if b.Client == nil {
		return nil
	}

	return b.Client.Close()
}

// AckMessage acknowledges the given message.
func (b *redisStreamsBroker) AckMessage(ctx context.Context, msg BrokerMessage) error {
	_, err := b.Client.XAck(ctx, msg.Topic(), msg.Group(), msg.ID()).Result()
	return err
}

// Options returns the broker options.
func (b *redisStreamsBroker) Options() *BrokerOptions {
	return b.options
}

// ************************************************
// * Publisher
// ************************************************

// Publish publishes the given message to given topics.
func (b *redisStreamsBroker) Publish(ctx context.Context, msg BrokerMessage, topics ...string) error {
	for _, topic := range topics {
		_, err := b.Client.XAdd(
			ctx,
			&redis.XAddArgs{
				Stream: topic,
				Values: map[string]interface{}{
					"body": msg.Body(),
				},
			}).Result()
		if err != nil {
			return err
		}
	}

	return nil
}

// ************************************************
// * Consumer
// ************************************************

type redisStreamsConsumer struct {
	// Topics is the list of topics to consume.
	topics []string

	// ConsumerGroup is the consumer group to consume from.
	consumerGroup string

	// broker
	broker *redisStreamsBroker

	// paused
	paused bool

	mu sync.RWMutex
}

// NewConsumer creates a new Consumer.
func (b *redisStreamsBroker) NewConsumer(opts ConsumerOptions) (Consumer, error) {
	return &redisStreamsConsumer{
		broker:        b,
		topics:        opts.Topics,
		consumerGroup: opts.ConsumerGroup,
	}, nil
}

// Consume consumes a message from the given topics.
// This sends messages to the returned channel.
func (c *redisStreamsConsumer) Consume(ctx context.Context) (<-chan BrokerMessage, error) {
	// create the consumer group if it doesn't exist
	_, err := c.broker.Client.XGroupCreateMkStream(ctx, c.topics[0], c.consumerGroup, "0").Result()
	if err != nil {
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return nil, err
		}
	}

	// create the channel to send messages to
	msgs := make(chan BrokerMessage)

	// create a new consumer
	go func() {
		defer close(msgs)

		for {
			// check if the consumer is paused
			c.loopUntilUnpaused()

			// consume a message from the topics
			res, err := c.broker.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    c.consumerGroup,
				Consumer: "consumer",
				Streams:  c.topics,
				Count:    1,
				Block:    0,
			}).Result()
			if err != nil {
				return
			}

			msg := &Message{}

			// using the methods to set the values
			// that we can get from the redis response
			// some are not relevant to the message regarding redis
			msg.SetBroker(c.broker)
			msg.SetTopic(res[0].Stream)
			msg.SetID(res[0].Messages[0].ID)
			msg.SetGroup(c.consumerGroup)
			msg.SetTimestamp(time.Now())

			body := res[0].Messages[0].Values["body"].(string)
			if body != "" {
				msg.SetBody([]byte(body))
			}

			// send the message to the channel
			msgs <- msg
		}
	}()

	return msgs, nil
}

func (c *redisStreamsConsumer) Pause() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = true
}

func (c *redisStreamsConsumer) Resume() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = false
}

func (c *redisStreamsConsumer) Topics() []string {
	return c.topics
}

func (c *redisStreamsConsumer) Close() error {
	// todo: should this be implemented?
	// the broker handles closing the connection
	return nil
}

func (c *redisStreamsConsumer) Connect(ctx context.Context) error {
	// todo: should this be implemented?
	// the broker is already connected
	return nil
}

// ConsumerGroup returns the consumer group that the consumer is consuming from.
func (c *redisStreamsConsumer) ConsumerGroup() string {
	return c.consumerGroup
}

// loopUntilUnpaused is a helper function that will loop until the consumer is unpaused.
func (c *redisStreamsConsumer) loopUntilUnpaused() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for c.paused {
		// sleep for a bit?
		time.Sleep(time.Millisecond * 100)
	}
}
