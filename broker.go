package gomb

// initalizations

// NewBroker creates a new MessageBroker.
func NewBroker(opts BrokerOptions) (Broker, error) {
	// var err error

	// check if broker type is set
	// and if it is valid
	t := opts.BrokerType
	if t == "" {
		return nil, ErrMissingBroker
	}

	// check auth is ok
	if opts.Auth != nil {
		// auth is not required
		if err := opts.Auth.Check(); err != nil {
			return nil, err
		}
	}

	// create the broker
	switch t {
	case BrokerTypeRedisStreams:
		// * Redis Streams
		b, err := newRedisStreamsBroker(opts)
		if err != nil {
			return nil, err
		}
		return b, nil
	case BrokerTypePulsar:
		// * Pulsar
		b, err := newPulsarBroker(opts)
		if err != nil {
			return nil, err
		}
		return b, nil
	default:
		return nil, ErrInvalidBroker
	}
}
