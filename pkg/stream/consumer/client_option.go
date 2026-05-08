package consumer

type ClientOption func(s clientSettings) clientSettings

func WithBootstrapServer(servers string) ClientOption {
	return func(s clientSettings) clientSettings {
		s.bootstrapServer = servers
		return s
	}
}
