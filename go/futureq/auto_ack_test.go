package futureq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	pb "github.com/futureq-io/protocol/proto/go"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type autoAckClientStream struct {
	grpc.ClientStream
	remaining int
	frames    chan *pb.ConsumerFrame
	end       chan struct{}
}

func (s *autoAckClientStream) Send(frame *pb.ConsumerFrame) error { s.frames <- frame; return nil }
func (s *autoAckClientStream) Recv() (*pb.QueueMessage, error) {
	if s.remaining == 0 {
		<-s.end
		return nil, io.EOF
	}
	s.remaining--
	return &pb.QueueMessage{Topic: "orders", Payload: []byte{byte(s.remaining)}, DeliveryTag: []byte{byte(s.remaining)}}, nil
}

func TestConsumerAckModes(t *testing.T) {
	for _, auto := range []bool{false, true} {
		for _, concurrency := range []int{1, 3} {
			name := "manual"
			if auto {
				name = "auto"
			}
			t.Run(fmt.Sprintf("%s/concurrency=%d", name, concurrency), func(t *testing.T) {
				cfg := defaultConsumerConfig()
				if cfg.autoAck {
					t.Fatal("auto-ack enabled by default")
				}
				WithAutoAck(auto)(&cfg)
				WithConcurrency(concurrency)(&cfg)
				stream := &autoAckClientStream{remaining: 3, frames: make(chan *pb.ConsumerFrame, 3), end: make(chan struct{})}
				consumer := &Consumer{cfg: cfg, stream: stream}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				handled := make(chan struct{}, 3)
				done := make(chan error, 1)
				go func() {
					done <- consumer.Subscribe(ctx, func(d Delivery) error {
						defer func() { handled <- struct{}{} }()
						switch d.Payload[0] {
						case 1:
							return errors.New("handler failed")
						case 0:
							panic("handler panicked")
						default:
							return nil
						}
					})
				}()
				for i := 0; i < 3; i++ {
					select {
					case <-handled:
					case <-ctx.Done():
						t.Fatal("handlers did not finish")
					}
				}
				if !auto {
					seen := make(map[byte]bool)
					for i := 0; i < 3; i++ {
						select {
						case frame := <-stream.frames:
							ack := frame.GetAck()
							if ack == nil {
								t.Fatal("expected manual ACK/NACK")
							}
							seen[ack.DeliveryTag[0]] = ack.Success
						case <-ctx.Done():
							t.Fatal("missing manual ACK/NACK")
						}
					}
					if len(seen) != 3 || !seen[2] || seen[1] || seen[0] {
						t.Fatalf("wrong ACK/NACK results: %v", seen)
					}
				}
				close(stream.end)
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("Subscribe did not stop")
				}
				if len(stream.frames) != 0 {
					t.Fatal("unexpected manual frame after processing")
				}
			})
		}
	}
}

type autoAckBindingServer struct {
	pb.UnimplementedFutureQConsumerServer
	confirm bool
	reject  bool
	binds   chan *pb.SubscribeInit
}

func (s *autoAckBindingServer) Subscribe(stream grpc.BidiStreamingServer[pb.ConsumerFrame, pb.QueueMessage]) error {
	frame, err := stream.Recv()
	if err != nil {
		return err
	}
	init := frame.GetInit()
	s.binds <- init
	if s.reject && init.AutoAck {
		return status.Error(codes.PermissionDenied, "auto-ack disabled")
	}
	headers := metadata.Pairs("subscription-generation", "generation")
	if s.confirm && init.AutoAck {
		headers.Set("subscription-auto-ack", "true")
	}
	if err := stream.SendHeader(headers); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestAutoAckHandshake(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		auto, confirm, reject, wantError bool
	}{
		{"manual-legacy", false, false, false, false},
		{"auto-confirmed", true, true, false, false},
		{"auto-legacy", true, false, false, true},
		{"auto-rejected", true, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener := bufconn.Listen(1 << 20)
			defer listener.Close()
			mock := &autoAckBindingServer{confirm: tc.confirm, reject: tc.reject, binds: make(chan *pb.SubscribeInit, 2)}
			server := grpc.NewServer()
			pb.RegisterFutureQConsumerServer(server, mock)
			go server.Serve(listener)
			defer server.Stop()
			tracker := newTopologyTracker(nil, time.Hour, func(context.Context, string) (*grpc.ClientConn, error) {
				return grpc.NewClient("passthrough:///auto-ack", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
			})
			tracker.setLeader(1, "auto-ack", Topology{})
			client := &Client{tracker: tracker}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			consumer, err := client.NewConsumer(ctx, "orders", "workers", WithAutoAck(tc.auto))
			if (err != nil) != tc.wantError {
				t.Fatalf("NewConsumer error: %v", err)
			}
			if bind := <-mock.binds; bind.AutoAck != tc.auto {
				t.Fatalf("wrong init: %v", bind)
			}
			if tc.reject && status.Code(err) != codes.PermissionDenied {
				t.Fatalf("policy error lost: %v", err)
			}
			if err == nil {
				defer consumer.Close()
				// Rebinding must preserve the requested mode and renegotiate it.
				consumer.mu.Lock()
				err = consumer.reconnect(ctx)
				consumer.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				if bind := <-mock.binds; bind.AutoAck != tc.auto {
					t.Fatalf("reconnect lost auto-ack: %v", bind)
				}
			}
		})
	}
}

type ChunkedDeliverySuite struct{ suite.Suite }

func TestChunkedDeliverySuite(t *testing.T) {
	suite.Run(t, new(ChunkedDeliverySuite))
}

type chunkTestStream struct {
	grpc.ClientStream
	responses []*pb.QueueMessage
	ended     chan struct{}
	acked     chan *pb.AckRequest
}

func (s *chunkTestStream) Recv() (*pb.QueueMessage, error) {
	if len(s.responses) == 0 {
		<-s.ended
		return nil, io.EOF
	}
	message := s.responses[0]
	s.responses = s.responses[1:]
	return message, nil
}

func (s *chunkTestStream) Send(frame *pb.ConsumerFrame) error {
	s.acked <- frame.GetAck()
	return nil
}

func (s *ChunkedDeliverySuite) TestSubscribeDeliversAndAcknowledgesTheWholeMessageOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	want := &pb.QueueMessage{
		Topic: "orders", Payload: bytes.Repeat([]byte("futureq"), 800),
		DeliveryTag: []byte("opaque-tag"), Attempt: 2,
	}
	data, err := proto.Marshal(want)
	s.Require().NoError(err)
	stream := &chunkTestStream{ended: make(chan struct{}), acked: make(chan *pb.AckRequest, 1)}
	for len(data) > 0 {
		size := min(350, len(data))
		stream.responses = append(stream.responses, &pb.QueueMessage{ChunkData: data[:size], MoreChunks: size < len(data)})
		data = data[size:]
	}
	handled := make(chan Delivery, 1)
	consumer := &Consumer{cfg: defaultConsumerConfig(), stream: stream}
	done := make(chan error, 1)
	go func() {
		done <- consumer.Subscribe(ctx, func(message Delivery) error {
			handled <- message
			return nil
		})
	}()
	defer func() { close(stream.ended); <-done }()
	select {
	case ack := <-stream.acked:
		s.Require().NotNil(ack)
		s.True(ack.GetSuccess())
		s.Equal(want.GetDeliveryTag(), ack.GetDeliveryTag())
	case <-ctx.Done():
		s.FailNow("the complete delivery was not acknowledged")
	}
	message := <-handled
	s.Equal(want.GetPayload(), message.Payload)
	s.Equal(want.GetAttempt(), message.Attempt)
	s.Equal(want.GetTopic(), message.Topic)
}

func (s *ChunkedDeliverySuite) TestIncompleteFrameIsDiscarded() {
	first := true
	message, err := recvDeliveryMessage(func() (*pb.QueueMessage, error) {
		if first {
			first = false
			return &pb.QueueMessage{ChunkData: []byte("partial"), MoreChunks: true}, nil
		}
		return nil, io.EOF
	})
	s.ErrorIs(err, io.ErrUnexpectedEOF)
	s.Nil(message)
}
