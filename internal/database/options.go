package database

type Option func(*options) error

type options struct{}

func defaultOptions() options {
	return options{}
}

func applyOptions(opts ...Option) (options, error) {
	o := defaultOptions()
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return o, err
		}
	}
	return o, nil
}
