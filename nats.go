package event_nats

import (
	"fmt"
	"strings"
	"sync"

	"github.com/infrago/infra"
	"github.com/infrago/event"
	"github.com/nats-io/nats.go"
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
	}
)

func parseSetting(inst *event.Instance) natsSetting {
	cfg := inst.Config.Setting
	setting := natsSetting{
		URL:    nats.DefaultURL,
		Stream: "INFRAGOE",
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
		return err
	}
	c.client.Flush()
	return c.client.LastError()
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

	_, err = js.StreamInfo(c.setting.Stream)
	if err != nil {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     c.setting.Stream,
			Subjects: []string{c.setting.Stream + ".*"},
		})
		if err != nil {
			return err
		}
	}
	c.stream = js
	return nil
}

func (c *natsJSConnection) Close() error {
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
		var (
			sub *nats.Subscription
			err error
		)
		if group == "" {
			sub, err = c.stream.Subscribe(subject, func(msg *nats.Msg) {
				c.instance.Submit(func() {
					c.instance.Serve(eventName, msg.Data)
				})
			}, nats.DeliverNew())
		} else {
			consumer := jsConsumer(c.setting.Stream, eventName, group)
			sub, err = c.stream.QueueSubscribe(subject, consumer, func(msg *nats.Msg) {
				c.instance.Submit(func() {
					c.instance.Serve(eventName, msg.Data)
				})
			}, nats.Durable(consumer), nats.DeliverNew())
		}
		if err != nil {
			return err
		}
		c.subs = append(c.subs, sub)
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
	return err
}

func jsSubject(stream, name string) string {
	name = strings.ReplaceAll(name, ".", "_")
	return fmt.Sprintf("%s.%s", stream, name)
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

var _ event.Connection = (*natsConnection)(nil)
var _ event.Connection = (*natsJSConnection)(nil)
