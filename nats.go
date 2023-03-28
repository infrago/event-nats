package event_nats

import (
	"errors"
	"sync"

	"github.com/infrago/event"
	"github.com/nats-io/nats.go"
)

var (
	errInvalidConnection = errors.New("Invalid event connection.")
	errAlreadyRunning    = errors.New("Nats event is already running.")
	errNotRunning        = errors.New("Nats event is not running.")
)

type (
	natsDriver  struct{}
	natsConnect struct {
		mutex sync.RWMutex

		running bool
		actives int64

		instance *event.Instance
		setting  natsSetting

		client *nats.Conn

		events map[string]string
		subs   map[string]*nats.Subscription
	}
	//配置文件
	natsSetting struct {
		Url      string
		Username string
		Password string
	}

	defaultMsg struct {
		name string
		data []byte
	}
)

// 连接
func (driver *natsDriver) Connect(inst *event.Instance) (event.Connect, error) {
	//获取配置信息
	setting := natsSetting{
		Url: nats.DefaultURL, Username: "", Password: "",
	}

	if vv, ok := inst.Setting["url"].(string); ok {
		setting.Url = vv
	}
	if vv, ok := inst.Setting["server"].(string); ok {
		setting.Url = vv
	}

	if vv, ok := inst.Setting["user"].(string); ok {
		setting.Username = vv
	}
	if vv, ok := inst.Setting["username"].(string); ok {
		setting.Username = vv
	}
	if vv, ok := inst.Setting["pass"].(string); ok {
		setting.Password = vv
	}
	if vv, ok := inst.Setting["password"].(string); ok {
		setting.Password = vv
	}

	return &natsConnect{
		instance: inst, setting: setting,
		events: make(map[string]string, 0),
		subs:   make(map[string]*nats.Subscription, 0),
	}, nil
}

// 打开连接
func (this *natsConnect) Open() error {
	opts := []nats.Option{}
	if this.setting.Username != "" && this.setting.Password != "" {
		opts = append(opts, nats.UserInfo(this.setting.Username, this.setting.Password))
	}
	client, err := nats.Connect(this.setting.Url, opts...)
	if err != nil {
		return err
	}

	this.client = client

	return nil
}

func (this *natsConnect) Health() (event.Health, error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return event.Health{Workload: this.actives}, nil
}

// 关闭连接
func (this *natsConnect) Close() error {
	if this.client != nil {
		this.client.Close()
	}
	return nil
}

func (this *natsConnect) Register(name, group string) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.events[name] = group

	return nil
}

// 开始订阅者
func (this *natsConnect) Start() error {
	if this.running {
		return errAlreadyRunning
	}

	nc := this.client

	//监听队列
	for key, val := range this.events {
		subKey := key
		subGroup := val

		sub, err := nc.QueueSubscribe(subKey, subGroup, func(msg *nats.Msg) {
			// 走协程执行，不阻塞，因为是事件，和队列不同
			this.instance.Submit(func() {
				this.instance.Serve(subKey, msg.Data)
			})
		})

		if err != nil {
			this.Close()
			return err
		}

		//记录sub
		this.subs[key] = sub

	}

	this.running = true
	return nil
}

// 停止订阅
func (this *natsConnect) Stop() error {
	if false == this.running {
		return errNotRunning
	}

	for _, sub := range this.subs {
		sub.Unsubscribe()
	}

	this.running = false
	return nil
}

func (this *natsConnect) Publish(name string, data []byte) error {
	if this.client == nil {
		return errInvalidConnection
	}

	nc := this.client

	err := nc.Publish(name, data)

	//强制flush，避免消息丢失或缓存
	nc.Flush()

	return err
}
