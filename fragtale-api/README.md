# Fragtale API

This crate provides REST and WebSocket API.
It also serves the metrics and health-check.

The [OpenAPI](openapi.json) documentation lists all available API calls.

The Web Socket protocol uses JSON serialized objects described by the
`SubscriberCommand` and `SubscriberResponse` enums from the `fragtale-client` crate.
Each command has a dedicated Web Socket connection to avoid large messages from
starving smaller confirmations.
