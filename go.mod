module github.com/bamgoo/event-nats

go 1.25.3

require (
	github.com/bamgoo/bamgoo v0.0.0-00010101000000-000000000000
	github.com/bamgoo/event v0.0.0-00010101000000-000000000000
	github.com/nats-io/nats.go v1.47.0
)

replace github.com/bamgoo/bamgoo => ../bamgoo

replace github.com/bamgoo/event => ../event
