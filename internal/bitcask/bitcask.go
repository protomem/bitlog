package bitcask

type Options struct{}

func DefaultOptions() Options {
	return Options{}
}

type Bitcask struct {
	Opts Options
	Idx  *Index
	Jrn  *Journal
}

func New(opts Options) *Bitcask {
	return &Bitcask{
		// TODO
	}
}

func (*Bitcask) Close() error {
	return nil
}
