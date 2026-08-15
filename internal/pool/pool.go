package pool

import (
	"context"
	"errors"
	"sync/atomic"
)

var ErrPoolClosed = errors.New("pool is closed")

type Pool struct {
	maxActive int32
	active    int32
	// ... other fields
}

func (p *Pool) Get(ctx context.Context) (*Conn, error) {
	// Try to reserve a slot
	for {
		active := atomic.LoadInt32(&p.active)
		if active >= p.maxActive {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				// Wait for a slot or retry
			}
			return nil, errors.New("pool exhausted")
		}
		if atomic.CompareAndSwapInt32(&p.active, active, active+1) {
			break
		}
	}

	// Dial with context awareness
	conn, err := p.dial(ctx)
	if err != nil {
		atomic.AddInt32(&p.active, -1)
		return nil, err
	}

	return conn, nil
}

func (p *Pool) dial(ctx context.Context) (*Conn, error) {
	// Ensure we check context before dialing
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	conn, err := p.dialer.Dial(ctx)
	if err != nil {
		return nil, err
	}

	// Double check context after dial
	select {
	case <-ctx.Done():
		conn.Close()
		return nil, ctx.Err()
	default:
		return conn, nil
	}
}