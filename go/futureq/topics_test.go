package futureq

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	pb "github.com/futureq-io/protocol/proto/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type topicTestServer struct {
	pb.UnimplementedFutureQTopicServer
	created chan *pb.CreateTopicRequest
	listed  chan *pb.ListTopicsRequest
}

func (s *topicTestServer) CreateTopic(_ context.Context, request *pb.CreateTopicRequest) (*pb.Topic, error) {
	s.created <- request
	return &pb.Topic{Name: request.GetName()}, nil
}

func (s *topicTestServer) GetTopic(_ context.Context, request *pb.GetTopicRequest) (*pb.Topic, error) {
	if request.GetName() == "missing" {
		return nil, status.Error(codes.NotFound, "topic does not exist")
	}
	return &pb.Topic{Name: request.GetName()}, nil
}

func (s *topicTestServer) ListTopics(_ context.Context, request *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	s.listed <- request
	if request.GetPageToken() == "" {
		return &pb.ListTopicsResponse{Topics: []*pb.Topic{{Name: "email.send"}}, NextPageToken: "email.send"}, nil
	}
	return &pb.ListTopicsResponse{Topics: []*pb.Topic{{Name: "orders"}}}, nil
}

func TestTopicAPIs(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	defer listener.Close()
	server := grpc.NewServer()
	service := &topicTestServer{created: make(chan *pb.CreateTopicRequest, 1), listed: make(chan *pb.ListTopicsRequest, 2)}
	pb.RegisterFutureQTopicServer(server, service)
	go server.Serve(listener)
	defer server.Stop()

	const leader = "topic-test-leader"
	tracker := newTopologyTracker(nil, time.Hour, func(_ context.Context, address string) (*grpc.ClientConn, error) {
		if address != leader {
			return nil, errors.New("topic request was routed away from the leader")
		}
		return grpc.NewClient("passthrough:///topics",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
	})
	tracker.setLeader(1, leader, Topology{})
	client := &Client{tracker: tracker}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	topic, err := client.CreateTopic(ctx, "email.send", InitialSubscription{Group: "mailers"}, InitialSubscription{SubscriberID: "audit"})
	if err != nil || topic.Name != "email.send" {
		t.Fatalf("create topic: got %v, %v", topic, err)
	}
	if got := <-service.created; !proto.Equal(got, &pb.CreateTopicRequest{
		Name: "email.send", InitialSubscriptions: []*pb.InitialSubscription{{GroupId: "mailers"}, {SubscriberId: "audit"}},
	}) {
		t.Fatalf("unexpected create request: %v", got)
	}

	topic, err = client.GetTopic(ctx, "email.send")
	if err != nil || topic.Name != "email.send" {
		t.Fatalf("get topic: got %v, %v", topic, err)
	}
	if _, err := client.GetTopic(ctx, "missing"); status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}

	first, err := client.ListTopics(ctx, 1, "")
	if err != nil || len(first.Topics) != 1 || first.Topics[0].Name != "email.send" || first.NextPageToken != "email.send" {
		t.Fatalf("first page: got %v, %v", first, err)
	}
	second, err := client.ListTopics(ctx, 1, first.NextPageToken)
	if err != nil || len(second.Topics) != 1 || second.Topics[0].Name != "orders" || second.NextPageToken != "" {
		t.Fatalf("second page: got %v, %v", second, err)
	}
	for _, token := range []string{"", first.NextPageToken} {
		if got := <-service.listed; !proto.Equal(got, &pb.ListTopicsRequest{PageSize: 1, PageToken: token}) {
			t.Fatalf("unexpected list request: %v", got)
		}
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateTopic(ctx, "closed"); !errors.Is(err, ErrClosed) {
		t.Fatalf("create after close: got %v", err)
	}
}
