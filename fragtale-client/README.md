# Fragtale client library

This library provides and abstraction of the Fragtale REST and Web Socket APIs
to enable rapid micro-service development with very little effort.

## Target scope

Micro-services written in Rust running on Kubernetes where the client is running
on the same cluster as Fragtale.

## Integration

Add the following to `Cargo.toml`:

```text
[dependencies]
fragtale_client = { git = "https://github.com/mydriatech/fragtale.git", branch = "main" }
```

(Use a specific GIT tag for production builds.)

Use the asynchronous [event handler example](examples/event_handler.rs) as a
starting point and deploy the micro-service with projected serviceAccountToken
with an audience matching the Fragtale backend:

```text
apiVersion: apps/v1
kind: Deployment
...
spec:
  ..
  template:
    ..
    spec:
      ..
      containers:
        ..
          env:
          - name: UPKIT_REQ_PROC_WS_BACKEND_BASEURL
            value: "http://fragtale.fragtale-demo.svc:8081/api/v1"
          volumeMounts:
          - mountPath: /var/run/secrets/tokens
            name: service-account-token
      volumes:
      - name: service-account-token
        projected:
          sources:
          - serviceAccountToken:
              path: service-account
              expirationSeconds: 7200
              audience: fragtale.fragtale-demo.svc
```
