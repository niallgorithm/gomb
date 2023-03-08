package gomb

import (
	"encoding/json"
	"time"
)

type BrokerType string

const (
	BrokerTypeKafka        BrokerType = "kafka"
	BrokerTypePulsar       BrokerType = "pulsar"
	BrokerTypeRedisStreams BrokerType = "redisstreams"
)

// ************************************************
// * BrokerOptions
// ************************************************

// BrokerOptions represents the options for a message broker.
type BrokerOptions struct {
	// The type of message broker to use.
	// This is required.
	BrokerType BrokerType

	// The address of the message broker(s).
	// This is required.
	// If multiple it should be comma separated.
	Address string

	// Authentication
	// This is optional.
	// If not set, no authentication will be used.
	Auth *AuthOptions

	// OperationTimeout
	// This is optional, and defaults to 5 seconds.
	// Applies to Pulsar only.
	OperationTimeout time.Duration

	// ConnectionTimeout
	// This is optional, and defaults to 5 seconds.
	// Applies to Pulsar only.
	ConnectionTimeout time.Duration

	// Custom Marshaller
	// This is optional, and defaults to json.Marshal.
	// Override this if you want to use a custom marshaller for your messages.
	Marshal func(v interface{}) ([]byte, error)

	// Custom Unmarshaller
	// This is optional, and defaults to json.Unmarshal.
	// Override this if you want to use a custom unmarshaller for your messages.
	Unmarshal func(data []byte, v interface{}) error
}

// NewBrokerOptions returns a new BrokerOptions.
func NewBrokerOptions(brokerType BrokerType, address string) BrokerOptions {
	return BrokerOptions{
		BrokerType: brokerType,
		Address:    address,
		Marshal:    json.Marshal,
		Unmarshal:  json.Unmarshal,
	}
}

// SetMarshal sets the Marshal function.
func (o *BrokerOptions) SetMarshal(f func(v interface{}) ([]byte, error)) {
	o.Marshal = f
}

// SetUnmarshal sets the Unmarshal function.
func (o *BrokerOptions) SetUnmarshal(f func(data []byte, v interface{}) error) {
	o.Unmarshal = f
}

// ************************************************
// * ConsumerOptions
// ************************************************

// ConsumerOptions represents the options for a message consumer.
type ConsumerOptions struct {
	// The consumer group to consume from.
	// This is required.
	ConsumerGroup string

	// The topics to consume from.
	// This is required.
	Topics []string
}

// NewConsumerOptions returns a new ConsumerOptions.
func NewConsumerOptions(consumerGroup string, topics []string) ConsumerOptions {
	return ConsumerOptions{
		ConsumerGroup: consumerGroup,
		Topics:        topics,
	}
}
