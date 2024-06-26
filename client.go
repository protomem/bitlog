package bitlog

import "context"

var _ Storage = (*Client)(nil)

type Client struct{}

func Connect() (*Client, error) {
	return &Client{}, nil
}

func (c *Client) Keys(ctx context.Context) ([][]byte, error) {
	panic("unimplemented")
}

func (c *Client) Range(ctx context.Context, iterator func(key []byte, value []byte) bool) error {
	panic("unimplemented")
}

func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	panic("unimplemented")
}

func (c *Client) Set(ctx context.Context, key []byte, value []byte) error {
	panic("unimplemented")
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	panic("unimplemented")
}

func (c *Client) Merge(ctx context.Context, key []byte, value []byte) error {
	panic("unimplemented")
}

func (c *Client) Close(ctx context.Context) error {
	panic("unimplemented")
}
