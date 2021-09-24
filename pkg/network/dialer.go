package network

import (
	"context"
	"net"
	"time"
)

type DialContext func(ctx context.Context, network, address string) (net.Conn, error)

// DefaultDialContext returns a DialContext function from a network dialer with default options sets.
func DefaultClientDialContext() DialContext {
	return dialerWithDefaultOptions()
}

func dialerWithDefaultOptions() DialContext {
	nd := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	return nd.DialContext
}
