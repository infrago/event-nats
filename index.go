package event_nats

import (
	"github.com/infrago/event"
)

func Driver() event.Driver {
	return &natsDriver{}
}
func JsDriver() event.Driver {
	return &natsjsDriver{}
}

func init() {
	jsd := JsDriver()
	event.Register("nats", Driver())
	event.Register("natsjs", jsd)
	event.Register("nats-js", jsd)
}
