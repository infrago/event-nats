package event_nats

import (
	"strings"
	"sync"

	"github.com/infrago/event"
	"github.com/infrago/infra"
	"github.com/nats-io/nats.go"
)

type (
	natsjsDriver  struct{}
	natsjsConnect struct {
		mutex sync.RWMutex

		running bool
		actives int64

		instance *event.Instance
		setting  natsjsSetting

		client *nats.Conn
		stream nats.JetStreamContext

		events map[string]string
		subs   map[string]*nats.Subscription
	}
	//配置文件
	natsjsSetting struct {
		Stream   string
		Url      string
		Token    string
		Username string
		Password string
	}
)

// 连接
func (driver *natsjsDriver) Connect(inst *event.Instance) (event.Connect, error) {
	//获取配置信息
	setting := natsjsSetting{
		Stream: infra.INFRA,
		Url:    nats.DefaultURL,
	}

	if name := infra.Name(); name != "" {
		setting.Stream = strings.ToUpper(name)
	}

	if inst.Config.Prefix != "" {
		setting.Stream = strings.ToUpper(inst.Config.Prefix)
	}

	if vv, ok := inst.Config.Setting["url"].(string); ok {
		setting.Url = vv
	}
	if vv, ok := inst.Config.Setting["stream"].(string); ok {
		setting.Stream = strings.ToUpper(vv)
	}

	if vv, ok := inst.Setting["token"].(string); ok {
		setting.Token = vv
	}

	if vv, ok := inst.Config.Setting["user"].(string); ok {
		setting.Username = vv
	}
	if vv, ok := inst.Config.Setting["username"].(string); ok {
		setting.Username = vv
	}
	if vv, ok := inst.Config.Setting["pass"].(string); ok {
		setting.Password = vv
	}
	if vv, ok := inst.Config.Setting["password"].(string); ok {
		setting.Password = vv
	}

	//for event
	setting.Stream += "E"

	return &natsjsConnect{
		instance: inst, setting: setting,
		events: make(map[string]string, 0),
		subs:   make(map[string]*nats.Subscription, 0),
	}, nil
}

// 打开连接
func (this *natsjsConnect) Open() error {
	opts := []nats.Option{}
	if this.setting.Token != "" {
		opts = append(opts, nats.Token(this.setting.Token))
	}
	if this.setting.Username != "" && this.setting.Password != "" {
		opts = append(opts, nats.UserInfo(this.setting.Username, this.setting.Password))
	}

	client, err := nats.Connect(this.setting.Url, opts...)
	if err != nil {
		return err
	}
	this.client = client

	js, err := client.JetStream()
	if err != nil {
		return err
	}

	//处理stream
	_, err = js.StreamInfo(this.setting.Stream)
	if err != nil {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     this.setting.Stream,
			Subjects: []string{this.setting.Stream + ".*"},
		})

		if err != nil {
			return err
		}
	}

	this.stream = js

	return nil
}

func (this *natsjsConnect) Health() (event.Health, error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return event.Health{Workload: this.actives}, nil
}

// 关闭连接
func (this *natsjsConnect) Close() error {
	if this.client != nil {
		this.client.Close()
	}
	return nil
}

func (this *natsjsConnect) Register(name, group string) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.events[name] = group

	return nil
}

// 开始订阅者
func (this *natsjsConnect) Start() error {
	if this.running {
		return errAlreadyRunning
	}

	js := this.stream

	//监听队列
	for key, val := range this.events {
		eventName := key
		eventGroup := val

		//消费者，不一样，就可以每一个都接收到了
		//所以，不需要折腾group，直接按消费者来
		realName := subName(eventName, this.setting.Stream)
		realConsumer := subConsumer(eventName, this.setting.Stream)

		realConsumer = realConsumer + "_" + eventGroup

		opts := []nats.SubOpt{
			nats.Durable(realConsumer),
			nats.DeliverNew(),
		}

		sub, err := js.QueueSubscribe(realName, realConsumer, func(msg *nats.Msg) {
			// 走协程执行，不阻塞，因为是事件，和队列不同
			this.instance.Submit(func() {
				this.instance.Serve(eventName, msg.Data)
			})
		}, opts...)

		if err != nil {
			return err
		}

		this.subs[key] = sub
	}

	this.running = true
	return nil
}

// 停止订阅
func (this *natsjsConnect) Stop() error {
	if false == this.running {
		return errNotRunning
	}

	//监听队列
	for _, sub := range this.subs {
		sub.Unsubscribe()
	}

	this.running = false
	return nil
}

func (this *natsjsConnect) Publish(name string, data []byte) error {
	if this.client == nil {
		return errInvalidConnection
	}

	js := this.stream

	//因为jetstream不支持点.所有处理一下
	name = subName(name, this.setting.Stream)

	_, err := js.Publish(name, data)

	if err != nil {
		return err
	}

	return err
}
