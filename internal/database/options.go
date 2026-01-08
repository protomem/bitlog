package database

import "errors"

type Option func(*options) error

func WithRootPath(path string) Option {
	return func(o *options) error {
		o.RootPath = path
		return nil
	}
}

type options struct {
	RootPath string
}

func defaultOptions() options {
	return options{
		RootPath: "",
	}
}

func applyOptions(opts ...Option) (options, error) {
	o := defaultOptions()

	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return o, err
		}
	}

	if err := o.validate(); err != nil {
		return o, err
	}

	return o, nil
}

func (opts options) validate() error {
	if opts.RootPath == "" {
		return errors.New("root path cannot be empty")
	}

	return nil
}
