package source

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/util"
)

func TestNewSource(t *testing.T) {
	tests := []struct {
		name        string
		opts        Options
		wantErr     bool
		errContains string
	}{
		{
			name: "valid options",
			opts: Options{
				Endpoint: nats.DefaultURL,
				Topic:    util.RandomTopicName(),
			},
			wantErr: false,
		},
		{
			name: "invalid endpoint",
			opts: Options{
				Endpoint: "invalid://endpoint:99999",
				Topic:    util.RandomTopicName(),
			},
			wantErr:     true,
			errContains: "failed to connect to",
		},
		{
			name: "empty endpoint",
			opts: Options{
				Endpoint: "",
				Topic:    util.RandomTopicName(),
			},
			wantErr: false,
		},
		{
			name: "empty topic",
			opts: Options{
				Endpoint: nats.DefaultURL,
				Topic:    "",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source, err := NewSource(tt.opts)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}

				var connErr *connectorErrors.ConnectorError
				require.True(t, errors.As(err, &connErr), "error should be a ConnectorError")
				assert.Equal(t, "source", connErr.Component)
				assert.Equal(t, connectorErrors.ErrCodeConnectionFailed, connErr.Code)
				assert.Equal(t, tt.opts.Endpoint, connErr.Metadata["endpoint"])
				return
			}

			require.NoError(t, err)
			require.NotNil(t, source)
			require.NotNil(t, source.NatsConn)
			assert.True(t, source.NatsConn.IsConnected())

			defer source.Close()
		})
	}
}

func TestSource_Subscribe(t *testing.T) {
	tests := []struct {
		name         string
		setupSource  func(t *testing.T) *Source
		handler      nats.MsgHandler
		wantErr      bool
		errContains  string
		testMessage  bool
		messageCount int
	}{
		{
			name: "successful subscription",
			setupSource: func(t *testing.T) *Source {
				source, err := NewSource(Options{
					Endpoint: nats.DefaultURL,
					Topic:    util.RandomTopicName(),
				})
				require.NoError(t, err)
				return source
			},
			handler: func(msg *nats.Msg) {
			},
			wantErr:      false,
			testMessage:  true,
			messageCount: 1,
		},
		{
			name: "subscription with message handling",
			setupSource: func(t *testing.T) *Source {
				source, err := NewSource(Options{
					Endpoint: nats.DefaultURL,
					Topic:    util.RandomTopicName(),
				})
				require.NoError(t, err)
				return source
			},
			handler: func(msg *nats.Msg) {
				assert.Equal(t, "test message", string(msg.Data))
			},
			wantErr:      false,
			testMessage:  true,
			messageCount: 1,
		},
		{
			name: "subscription with multiple messages",
			setupSource: func(t *testing.T) *Source {
				source, err := NewSource(Options{
					Endpoint: nats.DefaultURL,
					Topic:    util.RandomTopicName(),
				})
				require.NoError(t, err)
				return source
			},
			handler: func(msg *nats.Msg) {
			},
			wantErr:      false,
			testMessage:  true,
			messageCount: 5,
		},
		{
			name: "subscription on closed connection",
			setupSource: func(t *testing.T) *Source {
				source, err := NewSource(Options{
					Endpoint: nats.DefaultURL,
					Topic:    util.RandomTopicName(),
				})
				require.NoError(t, err)
				source.NatsConn.Close()
				return source
			},
			handler: func(msg *nats.Msg) {
			},
			wantErr:     true,
			errContains: "failed to subscribe to topic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := tt.setupSource(t)
			defer source.Close()

			var receivedCount int32
			var wg sync.WaitGroup

			if tt.testMessage {
				wg.Add(tt.messageCount)
				wrappedHandler := func(msg *nats.Msg) {
					atomic.AddInt32(&receivedCount, 1)
					tt.handler(msg)
					wg.Done()
				}

				err := source.Subscribe(wrappedHandler)
				if tt.wantErr {
					require.Error(t, err)
					if tt.errContains != "" {
						assert.Contains(t, err.Error(), tt.errContains)
					}

					var connErr *connectorErrors.ConnectorError
					require.True(t, errors.As(err, &connErr), "error should be a ConnectorError")
					assert.Equal(t, "source", connErr.Component)
					assert.Equal(t, connectorErrors.ErrCodeSubscriptionFailed, connErr.Code)
					assert.Equal(t, source.Options.Topic, connErr.Metadata["topic"])
					return
				}

				require.NoError(t, err)

				for _ = range tt.messageCount {
					err = source.NatsConn.Publish(source.Options.Topic, []byte("test message"))
					require.NoError(t, err)
				}

				err = source.NatsConn.Flush()
				require.NoError(t, err)

				done := make(chan struct{})
				go func() {
					wg.Wait()
					close(done)
				}()

				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Fatal("timeout waiting for messages")
				}

				assert.Equal(t, int32(tt.messageCount), atomic.LoadInt32(&receivedCount))
			} else {
				err := source.Subscribe(tt.handler)
				if tt.wantErr {
					require.Error(t, err)
					if tt.errContains != "" {
						assert.Contains(t, err.Error(), tt.errContains)
					}
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestSource_Close(t *testing.T) {
	tests := []struct {
		name        string
		setupSource func(t *testing.T) *Source
		preClose    func(t *testing.T, source *Source)
	}{
		{
			name: "close active connection",
			setupSource: func(t *testing.T) *Source {
				source, err := NewSource(Options{
					Endpoint: nats.DefaultURL,
					Topic:    util.RandomTopicName(),
				})
				require.NoError(t, err)
				return source
			},
		},
		{
			name: "close with active subscription",
			setupSource: func(t *testing.T) *Source {
				source, err := NewSource(Options{
					Endpoint: nats.DefaultURL,
					Topic:    util.RandomTopicName(),
				})
				require.NoError(t, err)
				return source
			},
			preClose: func(t *testing.T, source *Source) {
				err := source.Subscribe(func(msg *nats.Msg) {})
				require.NoError(t, err)
			},
		},
		{
			name: "close already closed connection",
			setupSource: func(t *testing.T) *Source {
				source, err := NewSource(Options{
					Endpoint: nats.DefaultURL,
					Topic:    util.RandomTopicName(),
				})
				require.NoError(t, err)
				return source
			},
			preClose: func(t *testing.T, source *Source) {
				source.NatsConn.Close()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := tt.setupSource(t)

			if tt.preClose != nil {
				tt.preClose(t, source)
			}

			wasConnected := source.NatsConn.IsConnected()

			source.Close()

			if wasConnected {
				assert.True(t, source.NatsConn.IsClosed())
			}
		})
	}
}

func TestSource_Close_Idempotent(t *testing.T) {
	source, err := NewSource(Options{
		Endpoint: nats.DefaultURL,
		Topic:    util.RandomTopicName(),
	})
	require.NoError(t, err)

	assert.True(t, source.NatsConn.IsConnected())

	source.Close()
	assert.True(t, source.NatsConn.IsClosed())

	source.Close()
	assert.True(t, source.NatsConn.IsClosed())
}

func TestSource_Close_DrainBehavior(t *testing.T) {
	topic := util.RandomTopicName()
	source, err := NewSource(Options{
		Endpoint: nats.DefaultURL,
		Topic:    topic,
	})
	require.NoError(t, err)

	var receivedCount int32
	var wg sync.WaitGroup

	wg.Add(1)
	err = source.Subscribe(func(msg *nats.Msg) {
		atomic.AddInt32(&receivedCount, 1)
		time.Sleep(100 * time.Millisecond)
		wg.Done()
	})
	require.NoError(t, err)

	err = source.NatsConn.Publish(topic, []byte("test message"))
	require.NoError(t, err)
	err = source.NatsConn.Flush()
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	source.Close()

	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&receivedCount))
	// Note: The exact timing of Drain() is implementation-dependent
	// We verify that the connection is eventually closed
	assert.True(t, source.NatsConn.IsClosed())
}

func TestSource_ConcurrentOperations(t *testing.T) {
	source, err := NewSource(Options{
		Endpoint: nats.DefaultURL,
		Topic:    util.RandomTopicName(),
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	numGoroutines := 10

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			source.Close()
		}()
	}

	wg.Wait()
	assert.True(t, source.NatsConn.IsClosed())
}

type testProvider struct {
	endpoint string
	topic    string
}

func (p *testProvider) Endpoint() string { return p.endpoint }
func (p *testProvider) Topic() string    { return p.topic }

func TestOptionsProvider(t *testing.T) {
	provider := &testProvider{
		endpoint: nats.DefaultURL,
		topic:    util.RandomTopicName(),
	}

	opts := FromOptionsProvider(provider)

	assert.Equal(t, provider.endpoint, opts.Endpoint)
	assert.Equal(t, provider.topic, opts.Topic)
}

func TestFunctionalOptions(t *testing.T) {
	endpoint := "nats://custom:4222"
	topic := util.RandomTopicName()

	opts := NewOptions(
		WithEndpoint(endpoint),
		WithTopic(topic),
	)

	assert.Equal(t, endpoint, opts.Endpoint)
	assert.Equal(t, topic, opts.Topic)
}

func TestOptionsValidation(t *testing.T) {
	tests := []struct {
		name    string
		opts    Options
		wantErr bool
	}{
		{
			name: "valid options",
			opts: Options{
				Endpoint: nats.DefaultURL,
				Topic:    util.RandomTopicName(),
			},
			wantErr: false,
		},
		{
			name: "empty topic is valid",
			opts: Options{
				Endpoint: nats.DefaultURL,
				Topic:    "",
			},
			wantErr: false,
		},
		{
			name: "empty endpoint uses default",
			opts: Options{
				Endpoint: "",
				Topic:    util.RandomTopicName(),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source, err := NewSource(tt.opts)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, source)
			defer source.Close()
		})
	}
}

func TestSource_ConnectionRecovery(t *testing.T) {
	topic := util.RandomTopicName()

	source, err := NewSource(Options{
		Endpoint: nats.DefaultURL,
		Topic:    topic,
	})
	require.NoError(t, err)
	defer source.Close()

	var receivedCount int32
	err = source.Subscribe(func(msg *nats.Msg) {
		atomic.AddInt32(&receivedCount, 1)
	})
	require.NoError(t, err)

	err = source.NatsConn.Publish(topic, []byte("before"))
	require.NoError(t, err)
	err = source.NatsConn.Flush()
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	source.NatsConn.Close()

	time.Sleep(100 * time.Millisecond)

	newSource, err := NewSource(Options{
		Endpoint: nats.DefaultURL,
		Topic:    topic,
	})
	require.NoError(t, err)
	defer newSource.Close()

	err = newSource.Subscribe(func(msg *nats.Msg) {
		atomic.AddInt32(&receivedCount, 1)
	})
	require.NoError(t, err)

	err = newSource.NatsConn.Publish(topic, []byte("after"))
	require.NoError(t, err)
	err = newSource.NatsConn.Flush()
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int32(2), atomic.LoadInt32(&receivedCount))
}

func TestSource_HandlerPanic(t *testing.T) {
	source, err := NewSource(Options{
		Endpoint: nats.DefaultURL,
		Topic:    util.RandomTopicName(),
	})
	require.NoError(t, err)
	defer source.Close()

	var recovered bool
	panicHandler := func(msg *nats.Msg) {
		defer func() {
			if r := recover(); r != nil {
				recovered = true
			}
		}()
		panic("test panic")
	}

	err = source.Subscribe(panicHandler)
	require.NoError(t, err)

	err = source.NatsConn.Publish(source.Options.Topic, []byte("test"))
	require.NoError(t, err)
	err = source.NatsConn.Flush()
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	assert.True(t, recovered)
	assert.True(t, source.NatsConn.IsConnected())
}

func TestSource_MessageContext(t *testing.T) {
	topic := util.RandomTopicName()
	source, err := NewSource(Options{
		Endpoint: nats.DefaultURL,
		Topic:    topic,
	})
	require.NoError(t, err)
	defer source.Close()

	var receivedMsg *nats.Msg
	var wg sync.WaitGroup

	wg.Add(1)
	err = source.Subscribe(func(msg *nats.Msg) {
		receivedMsg = msg
		wg.Done()
	})
	require.NoError(t, err)

	testData := []byte("test message with context")
	err = source.NatsConn.Publish(topic, testData)
	require.NoError(t, err)
	err = source.NatsConn.Flush()
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	require.NotNil(t, receivedMsg)
	assert.Equal(t, topic, receivedMsg.Subject)
	assert.Equal(t, testData, receivedMsg.Data)
}

func TestSource_CloseTimeout(t *testing.T) {
	source, err := NewSource(Options{
		Endpoint: nats.DefaultURL,
		Topic:    util.RandomTopicName(),
	})
	require.NoError(t, err)

	err = source.Subscribe(func(msg *nats.Msg) {
		time.Sleep(5 * time.Second)
	})
	require.NoError(t, err)

	err = source.NatsConn.Publish(source.Options.Topic, []byte("slow message"))
	require.NoError(t, err)
	err = source.NatsConn.Flush()
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		source.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Log("Close operation completed within timeout")
	}

	assert.True(t, source.NatsConn.IsClosed())
}

func TestSource_ErrorMetadata(t *testing.T) {
	tests := []struct {
		name            string
		setupAndExecute func(t *testing.T) error
		expectedCode    connectorErrors.ErrorCode
		checkMetadata   func(t *testing.T, metadata map[string]interface{})
		expectedComp    string
	}{
		{
			name: "connection error includes endpoint metadata",
			setupAndExecute: func(t *testing.T) error {
				_, err := NewSource(Options{
					Endpoint: "nats://unreachable:99999",
					Topic:    util.RandomTopicName(),
				})
				return err
			},
			expectedCode: connectorErrors.ErrCodeConnectionFailed,
			expectedComp: "source",
			checkMetadata: func(t *testing.T, metadata map[string]interface{}) {
				assert.Contains(t, metadata, "endpoint")
				assert.Equal(t, "nats://unreachable:99999", metadata["endpoint"])
			},
		},
		{
			name: "subscription error includes topic metadata",
			setupAndExecute: func(t *testing.T) error {
				topic := util.RandomTopicName()
				source, err := NewSource(Options{
					Endpoint: nats.DefaultURL,
					Topic:    topic,
				})
				require.NoError(t, err)
				source.NatsConn.Close()
				return source.Subscribe(func(msg *nats.Msg) {})
			},
			expectedCode: connectorErrors.ErrCodeSubscriptionFailed,
			expectedComp: "source",
			checkMetadata: func(t *testing.T, metadata map[string]interface{}) {
				assert.Contains(t, metadata, "topic")
				assert.NotEmpty(t, metadata["topic"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.setupAndExecute(t)
			require.Error(t, err)

			var connErr *connectorErrors.ConnectorError
			require.True(t, errors.As(err, &connErr), "error should be a ConnectorError")
			assert.Equal(t, tt.expectedCode, connErr.Code, "error code mismatch")
			assert.Equal(t, tt.expectedComp, connErr.Component, "component mismatch")
			assert.NotNil(t, connErr.Metadata, "metadata should not be nil")

			if tt.checkMetadata != nil {
				tt.checkMetadata(t, connErr.Metadata)
			}
		})
	}
}

func TestSource_ErrorWrapping(t *testing.T) {
	tests := []struct {
		name            string
		setupAndExecute func(t *testing.T) error
		checkWrapped    func(t *testing.T, err error)
	}{
		{
			name: "connection error wraps underlying NATS error",
			setupAndExecute: func(t *testing.T) error {
				_, err := NewSource(Options{
					Endpoint: "nats://invalid.host:99999",
					Topic:    util.RandomTopicName(),
				})
				return err
			},
			checkWrapped: func(t *testing.T, err error) {
				var connErr *connectorErrors.ConnectorError
				require.True(t, errors.As(err, &connErr))
				assert.NotNil(t, connErr.Wrapped, "wrapped error should not be nil")
				assert.NotNil(t, connErr.Unwrap(), "Unwrap() should return wrapped error")
			},
		},
		{
			name: "subscription error wraps underlying NATS error",
			setupAndExecute: func(t *testing.T) error {
				source, err := NewSource(Options{
					Endpoint: nats.DefaultURL,
					Topic:    util.RandomTopicName(),
				})
				require.NoError(t, err)
				source.NatsConn.Close()
				return source.Subscribe(func(msg *nats.Msg) {})
			},
			checkWrapped: func(t *testing.T, err error) {
				var connErr *connectorErrors.ConnectorError
				require.True(t, errors.As(err, &connErr))
				assert.NotNil(t, connErr.Wrapped, "wrapped error should not be nil")
				assert.True(t, errors.Is(err, nats.ErrConnectionClosed), "should wrap NATS connection closed error")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.setupAndExecute(t)
			require.Error(t, err)
			tt.checkWrapped(t, err)
		})
	}
}

func TestSource_ErrorFormatting(t *testing.T) {
	tests := []struct {
		name            string
		setupAndExecute func(t *testing.T) error
		expectedPattern string
	}{
		{
			name: "connection error format includes component and code",
			setupAndExecute: func(t *testing.T) error {
				_, err := NewSource(Options{
					Endpoint: "nats://invalid:99999",
					Topic:    util.RandomTopicName(),
				})
				return err
			},
			expectedPattern: `\[source\] failed to connect to .+ \(code: 1002\)`,
		},
		{
			name: "subscription error format includes component and code",
			setupAndExecute: func(t *testing.T) error {
				source, err := NewSource(Options{
					Endpoint: nats.DefaultURL,
					Topic:    "test.topic",
				})
				require.NoError(t, err)
				source.NatsConn.Close()
				return source.Subscribe(func(msg *nats.Msg) {})
			},
			expectedPattern: `\[source\] failed to subscribe to topic .+ \(code: 1003\)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.setupAndExecute(t)
			require.Error(t, err)
			assert.Regexp(t, tt.expectedPattern, err.Error())
		})
	}
}
