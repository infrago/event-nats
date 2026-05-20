package event_nats

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/infrago/event"
	"github.com/infrago/infra"
	"github.com/nats-io/nats.go"
)

const (
	natsDefaultTimeout    = 5 * time.Second
	natsDefaultFlush      = 5 * time.Second
	natsDefaultAckWait    = 30 * time.Second
	natsDefaultDeadLetter = "dead"
	natsDefaultMaxDeliver = 0
	natsDefaultRetryDelay = time.Duration(0)
)

func init() {
	infra.Register("nats", &natsDriver{})
	js := &natsJSDriver{}
	infra.Register("natsjs", js)
	infra.Register("nats-js", js)
	infra.Register("jetstream", js)
}

type (
	natsDriver struct{}

	natsConnection struct {
		mutex    sync.RWMutex
		running  bool
		instance *event.Instance
		setting  natsSetting
		client   *nats.Conn
		events   map[string]string
		subs     []*nats.Subscription
	}

	natsJSDriver struct{}

	natsJSConnection struct {
		mutex    sync.RWMutex
		running  bool
		instance *event.Instance
		setting  natsSetting
		client   *nats.Conn
		stream   nats.JetStreamContext
		events   map[string]string
		subs     []*nats.Subscription
	}

	natsSetting struct {
		URL        string
		Token      string
		Username   string
		Password   string
		Stream     string
		QueueGroup string
		Timeout    time.Duration
		Flush      time.Duration
		AckWait    time.Duration
		MaxDeliver int
		RetryDelay time.Duration
		DeadLetter string
	}

	syncInstance interface {
		ServeSync(string, []byte) bool
	}

	deadLetterEnvelope struct {
		Data     []byte `json:"data"`
		Subject  string `json:"subject"`
		Source   string `json:"source"`
		Message  uint64 `json:"message"`
		Attempt  uint64 `json:"attempt"`
		Driver   string `json:"driver"`
		Datetime int64  `json:"datetime"`
	}
)

func parseSetting(inst *event.Instance) natsSetting {
	cfg := inst.Config.Setting
	setting := natsSetting{
		URL:        nats.DefaultURL,
		Stream:     "INFRAGOE",
		Timeout:    natsDefaultTimeout,
		Flush:      natsDefaultFlush,
		AckWait:    natsDefaultAckWait,
		MaxDeliver: natsDefaultMaxDeliver,
		RetryDelay: natsDefaultRetryDelay,
		DeadLetter: natsDefaultDeadLetter,
	}

	if v, ok := cfg["url"].(string); ok && v != "" {
		setting.URL = v
	}
	if v, ok := cfg["server"].(string); ok && v != "" {
		setting.URL = v
	}
	if v, ok := cfg["token"].(string); ok {
		setting.Token = v
	}
	if v, ok := cfg["user"].(string); ok && v != "" {
		setting.Username = v
	}
	if v, ok := cfg["username"].(string); ok && v != "" {
		setting.Username = v
	}
	if v, ok := cfg["pass"].(string); ok {
		setting.Password = v
	}
	if v, ok := cfg["password"].(string); ok {
		setting.Password = v
	}
	if v, ok := cfg["stream"].(string); ok && v != "" {
		setting.Stream = strings.ToUpper(v)
	}
	if v, ok := cfg["group"].(string); ok && v != "" {
		setting.QueueGroup = v
	}
	setting.Timeout = durationSetting(cfg, "timeout", setting.Timeout)
	setting.Flush = durationSetting(cfg, "flush_timeout", setting.Flush)
	setting.AckWait = durationSetting(cfg, "ack_wait", setting.AckWait)
	setting.MaxDeliver = intSetting(cfg, "max_deliver", setting.MaxDeliver)
	setting.RetryDelay = durationSetting(cfg, "retry_delay", setting.RetryDelay)
	setting.DeadLetter = stringSetting(cfg, "dead_letter", setting.DeadLetter)

	return setting
}

func connectNats(setting natsSetting) (*nats.Conn, error) {
	opts := make([]nats.Option, 0)
	if setting.Token != "" {
		opts = append(opts, nats.Token(setting.Token))
	}
	if setting.Username != "" || setting.Password != "" {
		opts = append(opts, nats.UserInfo(setting.Username, setting.Password))
	}
	if setting.Timeout > 0 {
		opts = append(opts, nats.Timeout(setting.Timeout))
	}
	return nats.Connect(setting.URL, opts...)
}

func (d *natsDriver) Connect(inst *event.Instance) (event.Connection, error) {
	return &natsConnection{
		instance: inst,
		setting:  parseSetting(inst),
		events:   make(map[string]string, 0),
		subs:     make([]*nats.Subscription, 0),
	}, nil
}

func (c *natsConnection) Open() error {
	nc, err := connectNats(c.setting)
	if err != nil {
		return err
	}
	c.client = nc
	return nil
}

func (c *natsConnection) Close() error {
	_ = c.Stop()
	if c.client != nil {
		c.client.Close()
	}
	return nil
}

func (c *natsConnection) Register(name, group string) error {
	c.mutex.Lock()
	c.events[name] = group
	c.mutex.Unlock()
	return nil
}

func (c *natsConnection) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.running {
		return nil
	}

	for name, group := range c.events {
		subject := name
		var (
			sub *nats.Subscription
			err error
		)
		if group == "" {
			sub, err = c.client.Subscribe(subject, func(msg *nats.Msg) {
				c.instance.Submit(func() {
					c.instance.Serve(subject, msg.Data)
				})
			})
		} else {
			queue := group
			if c.setting.QueueGroup != "" {
				queue = c.setting.QueueGroup + "." + group
			}
			sub, err = c.client.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
				c.instance.Submit(func() {
					c.instance.Serve(subject, msg.Data)
				})
			})
		}
		if err != nil {
			return err
		}
		c.subs = append(c.subs, sub)
	}
	if err := c.client.FlushTimeout(c.setting.Flush); err != nil {
		return err
	}
	c.running = true
	return nil
}

func (c *natsConnection) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.running {
		return nil
	}
	for _, sub := range c.subs {
		_ = sub.Unsubscribe()
	}
	c.subs = nil
	c.running = false
	return nil
}

func (c *natsConnection) Publish(name string, data []byte) error {
	if err := c.client.Publish(name, data); err != nil {
		traceEvent("publish", name, err, map[string]any{"driver": "nats", "bytes": len(data), "reliable": false})
		return err
	}
	err := c.client.FlushTimeout(c.setting.Flush)
	if err == nil {
		err = c.client.LastError()
	}
	traceEvent("publish", name, err, map[string]any{"driver": "nats", "bytes": len(data), "reliable": false})
	return err
}

func (d *natsJSDriver) Connect(inst *event.Instance) (event.Connection, error) {
	return &natsJSConnection{
		instance: inst,
		setting:  parseSetting(inst),
		events:   make(map[string]string, 0),
		subs:     make([]*nats.Subscription, 0),
	}, nil
}

func (c *natsJSConnection) Open() error {
	nc, err := connectNats(c.setting)
	if err != nil {
		return err
	}
	c.client = nc

	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	info, err := js.StreamInfo(c.setting.Stream)
	if err != nil {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     c.setting.Stream,
			Subjects: []string{c.setting.Stream + ".*"},
		})
		if err != nil {
			return err
		}
	} else if !streamHasSubject(info.Config.Subjects, c.setting.Stream+".*") {
		cfg := info.Config
		cfg.Subjects = append(cfg.Subjects, c.setting.Stream+".*")
		if _, err := js.UpdateStream(&cfg); err != nil {
			return err
		}
	}
	c.stream = js
	return nil
}

func (c *natsJSConnection) Close() error {
	_ = c.Stop()
	if c.client != nil {
		c.client.Close()
	}
	return nil
}

func (c *natsJSConnection) Register(name, group string) error {
	c.mutex.Lock()
	c.events[name] = group
	c.mutex.Unlock()
	return nil
}

func (c *natsJSConnection) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.running {
		return nil
	}

	for eventName, group := range c.events {
		subject := jsSubject(c.setting.Stream, eventName)
		opts := []nats.SubOpt{nats.DeliverNew(), nats.ManualAck()}
		if c.setting.AckWait > 0 {
			opts = append(opts, nats.AckWait(c.setting.AckWait))
		}
		if c.setting.MaxDeliver > 0 {
			opts = append(opts, nats.MaxDeliver(c.setting.MaxDeliver))
		}
		var (
			sub *nats.Subscription
			err error
		)
		if group == "" {
			sub, err = c.stream.Subscribe(subject, func(msg *nats.Msg) {
				c.instance.Submit(func() {
					if serveEvent(c.instance, eventName, msg.Data) {
						_ = msg.Ack()
						traceEvent("ack", eventName, nil, map[string]any{"driver": "natsjs", "subject": subject, "attempt": messageAttempt(msg), "bytes": len(msg.Data)})
					} else {
						c.handleFailedMessage(eventName, msg)
					}
				})
			}, opts...)
		} else {
			consumer := jsConsumer(c.setting.Stream, eventName, group)
			handler := func(msg *nats.Msg) {
				c.instance.Submit(func() {
					if serveEvent(c.instance, eventName, msg.Data) {
						_ = msg.Ack()
						traceEvent("ack", eventName, nil, map[string]any{"driver": "natsjs", "subject": subject, "consumer": consumer, "attempt": messageAttempt(msg), "bytes": len(msg.Data)})
					} else {
						c.handleFailedMessage(eventName, msg)
					}
				})
			}
			sub, err = c.queueSubscribe(subject, consumer, handler, opts...)
		}
		if err != nil {
			return err
		}
		c.subs = append(c.subs, sub)
	}
	if err := c.client.FlushTimeout(c.setting.Flush); err != nil {
		return err
	}
	c.running = true
	return nil
}

func (c *natsJSConnection) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.running {
		return nil
	}
	for _, sub := range c.subs {
		_ = sub.Unsubscribe()
	}
	c.subs = nil
	c.running = false
	return nil
}

func (c *natsJSConnection) Publish(name string, data []byte) error {
	_, err := c.stream.Publish(jsSubject(c.setting.Stream, name), data)
	traceEvent("publish", name, err, map[string]any{"driver": "natsjs", "subject": jsSubject(c.setting.Stream, name), "bytes": len(data), "reliable": true})
	return err
}

func (c *natsJSConnection) queueSubscribe(subject, consumer string, handler nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	sub, err := c.stream.QueueSubscribe(subject, consumer, handler, append(opts, nats.Durable(consumer))...)
	if err == nil || !isDurableConfigError(err) {
		return sub, err
	}
	if delErr := c.stream.DeleteConsumer(c.setting.Stream, consumer); delErr != nil {
		traceEvent("consumer_delete", subject, delErr, map[string]any{"driver": "natsjs", "consumer": consumer})
		return sub, err
	}
	traceEvent("consumer_delete", subject, nil, map[string]any{"driver": "natsjs", "consumer": consumer})
	return c.stream.QueueSubscribe(subject, consumer, handler, append(opts, nats.Durable(consumer))...)
}

func jsSubject(stream, name string) string {
	name = base64.RawURLEncoding.EncodeToString([]byte(name))
	return fmt.Sprintf("%s.%s", stream, name)
}

func (c *natsJSConnection) handleFailedMessage(eventName string, msg *nats.Msg) {
	delivered := uint64(1)
	streamSeq := uint64(0)
	if meta, err := msg.Metadata(); err == nil && meta != nil {
		delivered = meta.NumDelivered
		streamSeq = meta.Sequence.Stream
	}
	if c.setting.MaxDeliver > 0 && delivered >= uint64(c.setting.MaxDeliver) && c.setting.DeadLetter != "" {
		payload, err := json.Marshal(deadLetterEnvelope{
			Data:     msg.Data,
			Subject:  eventName,
			Source:   msg.Subject,
			Message:  streamSeq,
			Attempt:  delivered,
			Driver:   "natsjs",
			Datetime: time.Now().Unix(),
		})
		if err == nil {
			_, err = c.stream.Publish(jsSubject(c.setting.Stream, deadLetterSubject(c.setting.DeadLetter, eventName)), payload)
		}
		traceEvent("dead_letter", eventName, err, map[string]any{"driver": "natsjs", "attempt": delivered, "bytes": len(msg.Data)})
		if err == nil {
			_ = msg.Term()
			return
		}
	}
	var err error
	if c.setting.RetryDelay > 0 {
		err = msg.NakWithDelay(c.setting.RetryDelay)
	} else {
		err = msg.Nak()
	}
	traceEvent("nak", eventName, err, map[string]any{"driver": "natsjs", "attempt": delivered, "bytes": len(msg.Data)})
}

func messageAttempt(msg *nats.Msg) uint64 {
	if msg == nil {
		return 1
	}
	if meta, err := msg.Metadata(); err == nil && meta != nil && meta.NumDelivered > 0 {
		return meta.NumDelivered
	}
	return 1
}

func isDurableConfigError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "consumer") && (strings.Contains(msg, "configuration") || strings.Contains(msg, "config"))
}

func deadLetterSubject(prefix, subject string) string {
	if strings.Contains(prefix, "{subject}") {
		return strings.ReplaceAll(prefix, "{subject}", subject)
	}
	return strings.TrimRight(prefix, ".") + "." + subject
}

func streamHasSubject(subjects []string, subject string) bool {
	for _, item := range subjects {
		if item == subject {
			return true
		}
	}
	return false
}

func jsConsumer(stream, name, group string) string {
	name = jsSubject(stream, name)
	name = strings.ReplaceAll(name, ".", "_")
	group = strings.ReplaceAll(group, ".", "_")
	if group == "" {
		group = "all"
	}
	return name + "_" + group
}

func serveEvent(inst *event.Instance, name string, data []byte) bool {
	if inst == nil {
		return false
	}
	syncInst, ok := any(inst).(syncInstance)
	if !ok {
		return false
	}
	return syncInst.ServeSync(name, data)
}

func traceEvent(operation, name string, err error, attrs map[string]any) {
	if attrs == nil {
		attrs = map[string]any{}
	}
	attrs["module"] = "event"
	attrs["operation"] = operation
	if err != nil {
		attrs["status"] = "error"
		attrs["error"] = err.Error()
	} else {
		attrs["status"] = "ok"
	}
	_ = infra.NewMeta().Trace("event:"+name, infra.TraceAttrs("infrago", infra.TraceKindEvent, name, attrs))
}

func durationSetting(setting map[string]any, key string, def time.Duration) time.Duration {
	switch v := setting[key].(type) {
	case time.Duration:
		if v >= 0 {
			return v
		}
	case int:
		if v >= 0 {
			return time.Duration(v) * time.Second
		}
	case int64:
		if v >= 0 {
			return time.Duration(v) * time.Second
		}
	case float64:
		if v >= 0 {
			return time.Duration(v * float64(time.Second))
		}
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return def
		}
		if d, err := time.ParseDuration(text); err == nil && d >= 0 {
			return d
		}
		if n, err := strconv.Atoi(text); err == nil && n >= 0 {
			return time.Duration(n) * time.Second
		}
	}
	return def
}

func intSetting(setting map[string]any, key string, def int) int {
	switch v := setting[key].(type) {
	case int:
		if v >= 0 {
			return v
		}
	case int64:
		if v >= 0 {
			return int(v)
		}
	case float64:
		if v >= 0 {
			return int(v)
		}
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

func stringSetting(setting map[string]any, key, def string) string {
	if v, ok := setting[key].(string); ok {
		return strings.TrimSpace(v)
	}
	return def
}

var _ event.Connection = (*natsConnection)(nil)
var _ event.Connection = (*natsJSConnection)(nil)
