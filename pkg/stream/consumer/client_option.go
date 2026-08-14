package consumer

// ClientOption configures the Kafka client built by NewFactory.
type ClientOption func(s clientSettings) clientSettings

// WithBootstrapServer sets the broker list, as a comma-separated
// "host:port" string.
func WithBootstrapServer(servers string) ClientOption {
	return func(s clientSettings) clientSettings {
		s.bootstrapServer = servers
		return s
	}
}
