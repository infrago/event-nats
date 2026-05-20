package event_nats

import (
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNatsJetStreamIntegration(t *testing.T) {
	if os.Getenv("EVENT_NATS_INTEGRATION") != "1" {
		t.Skip("set EVENT_NATS_INTEGRATION=1 to run")
	}
	url := os.Getenv("EVENT_NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream failed: %v", err)
	}

	stream := "EVENTTEST"
	subject := jsSubject(stream, "user.created")
	_ = js.DeleteStream(stream)
	if _, err := js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{stream + ".*"}}); err != nil {
		t.Fatalf("add stream failed: %v", err)
	}
	defer js.DeleteStream(stream)

	sub, err := js.SubscribeSync(subject, nats.ManualAck(), nats.DeliverAll(), nats.AckWait(time.Second), nats.MaxDeliver(2))
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Unsubscribe()

	if _, err := js.Publish(subject, []byte("body")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("next message failed: %v", err)
	}
	if string(msg.Data) != "body" {
		t.Fatalf("unexpected body %q", string(msg.Data))
	}
	if err := msg.Nak(); err != nil {
		t.Fatalf("nak failed: %v", err)
	}
	msg, err = sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("redelivered message failed: %v", err)
	}
	meta, err := msg.Metadata()
	if err != nil {
		t.Fatalf("metadata failed: %v", err)
	}
	if meta.NumDelivered != 2 {
		t.Fatalf("unexpected delivery attempt %d", meta.NumDelivered)
	}
	if err := msg.Term(); err != nil {
		t.Fatalf("term failed: %v", err)
	}
}
