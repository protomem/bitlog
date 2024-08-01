package bitlog

import "context"

var _ Storage = (*Client)(nil)

type Client struct{}

type ConnectOptions struct{}

func Connect(ctx context.Context, addr string, opts ConnectOptions) (*Client, error) {
	return &Client{}, nil
}

func (c *Client) Keys(ctx context.Context) ([]Key, error) {
	panic("unimplemented")
}

func (c *Client) Find(ctx context.Context, pattern Key) ([]Key, error) {
	panic("unimplemented")
}

func (c *Client) Get(ctx context.Context, key Key) (Value, error) {
	panic("unimplemented")
}

func (c *Client) Set(ctx context.Context, key Key, value Value) error {
	panic("unimplemented")
}

func (c *Client) Del(ctx context.Context, key Key) error {
	panic("unimplemented")
}

func (c *Client) Merge(ctx context.Context) error {
	panic("unimplemented")
}

func (c *Client) Sync(ctx context.Context) error {
	panic("unimplemented")
}

func (c *Client) Check(ctx context.Context) error {
	panic("unimplemented")
}

func (c *Client) Close(ctx context.Context) error {
	panic("unimplemented")
}
