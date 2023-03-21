package event_nats

import (
	"github.com/infrago/event"
	"github.com/infrago/infra"
)

func Driver() event.Driver {
	return &natsDriver{}
}
func JsDriver() event.Driver {
	return &natsjsDriver{}
}

func init() {
	infra.Register("nats", Driver())

	jsd := JsDriver()
	infra.Register("natsjs", jsd)
	infra.Register("nats-js", jsd)
}
