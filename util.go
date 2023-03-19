package event_nats

import (
	"fmt"
	"strings"
)

// func replaceName(name, stream string) string {
// 	name = strings.Replace(name, ".", "_", -1)

// 	prefix := fmt.Sprintf("%s.", stream)
// 	if false == strings.HasPrefix(strings.ToUpper(name), prefix) {
// 		name = prefix + name
// 	}

// 	return name
// }

func subName(name, stream string) string {
	prefix := fmt.Sprintf("%s.", stream)
	name = strings.Replace(name, ".", "_", -1)

	// prefix := ""
	// if stream != "" {
	// 	prefix = fmt.Sprintf("%s.", stream)
	// }
	// if false == strings.HasPrefix(strings.ToUpper(name), prefix) {
	// 	name = prefix + name
	// }

	//为了区分事件和队列，以免串了，加了后缀
	return fmt.Sprintf("%s%s", prefix, name)
}

func subConsumer(name, stream string) string {
	name = subName(name, stream)
	name = strings.Replace(name, ".", "_", -1)
	return name
}
