package event_nats

import (
	"strings"
	"testing"
	"time"

	"github.com/infrago/event"
)

func TestJetStreamSubjectEncodingAvoidsDotUnderscoreCollision(t *testing.T) {
	first := jsSubject("EVENTS", "user.created")
	second := jsSubject("EVENTS", "user_created")
	if first == second {
		t.Fatalf("expected distinct subjects, got %q", first)
	}
	if !strings.HasPrefix(first, "EVENTS.") || !strings.HasPrefix(second, "EVENTS.") {
		t.Fatalf("unexpected stream prefix: %q %q", first, second)
	}
}

func TestParseNatsSetting(t *testing.T) {
	setting := parseSetting(&event.Instance{
		Config: event.Config{
			Setting: map[string]any{
				"url":           "nats://example:4222",
				"stream":        "events",
				"timeout":       "250ms",
				"flush_timeout": 2,
				"ack_wait":      "3s",
				"max_deliver":   "5",
				"retry_delay":   "150ms",
				"dead_letter":   "dlq.{subject}",
			},
		},
	})

	if setting.URL != "nats://example:4222" {
		t.Fatalf("unexpected url %q", setting.URL)
	}
	if setting.Stream != "EVENTS" {
		t.Fatalf("unexpected stream %q", setting.Stream)
	}
	if setting.Timeout != 250*time.Millisecond {
		t.Fatalf("unexpected timeout %v", setting.Timeout)
	}
	if setting.Flush != 2*time.Second {
		t.Fatalf("unexpected flush %v", setting.Flush)
	}
	if setting.AckWait != 3*time.Second {
		t.Fatalf("unexpected ack wait %v", setting.AckWait)
	}
	if setting.MaxDeliver != 5 {
		t.Fatalf("unexpected max deliver %d", setting.MaxDeliver)
	}
	if setting.RetryDelay != 150*time.Millisecond {
		t.Fatalf("unexpected retry delay %v", setting.RetryDelay)
	}
	if got := deadLetterSubject(setting.DeadLetter, "user.created"); got != "dlq.user.created" {
		t.Fatalf("unexpected dead letter subject %q", got)
	}
}
