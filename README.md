# p1-relay

TCP relay for DSMR/P1 telegrams. One upstream connection, many consumers.

A P1 reader based on [esp-link](https://github.com/jeelabs/esp-link) accepts only
one TCP connection at a time. Point p1-relay at the reader and it fans the
telegram stream out to as many programs as you like: dsmr-reader, Home
Assistant, Node-RED, a home battery, all at once.

I use the "SlimmeLezer" by Marcel Zuidwijk (@zuidwijk), flashed with the esp-link firmware offered on its product page rather than the default ESPHome build, ESPHome parses the telegram on the device and does not hand out the raw stream. Any reader that exposes raw telegrams over TCP will work.

New consumers are only connected at a telegram boundary (`/`), so nobody ever
receives half a telegram. Existing consumers get the bytes unchanged, with no
added latency.

## Run it

```sh
podman run -d --name p1-relay \
  -e UPSTREAM_HOST=192.168.1.50 \
  -p 8888:8888 \
  ghcr.io/OWNER/p1-relay:latest
```

Then point your programs at port `8888` instead of the reader itself.
Disconnect any existing consumer from the reader first because it can only serve one.

Verify with:

```sh
nc localhost 8888 | head -40
```

## Configuration

All settings are environment variables. `UPSTREAM_HOST` is the only required one.

| Variable | Default | Description |
| --- | --- | --- |
| `UPSTREAM_HOST` | — | Address of the P1 reader |
| `UPSTREAM_PORT` | `23` | Port of the P1 reader |
| `LISTEN_ADDR` | `:8888` | Where consumers connect |
| `HEALTH_ADDR` | `:9090` | Serves `/healthz` |
