# Fragtale quick start guide

## Prerequisites

A Kubernetes installation with DNS, storage, cert-manager.

All the default values in the are adapted to enable testing on a local single
node variant like [k3s](https://k3s.io/), [microk8s](https://microk8s.io/)
or [minikube](https://minikube.sigs.k8s.io/docs/) is suitable for a local demo.

A normal production setup should have at least 3+ Cassandra nodes and it is
reasonable to use a replica count of 3 to have meaningful quorums.

Each deployed Cassandra node here will use up to 2 GiB of memory and 2 cores.

To use Cassandra's Light Weight Transactions which Fragtale depends on, at least
2 Cassandra nodes are required.

For this demo setup we will use 2 Cassandra nodes and a replication factor of 1
to spare the demo machines disk IO from duplicate writes.

## Install the Cassandra operator

```text
helm repo add k8ssandra https://helm.k8ssandra.io/stable
helm repo update
helm upgrade --install --atomic --create-namespace --namespace k8ssandra-operator \
    k8ssandra-operator k8ssandra/k8ssandra-operator \
    --set global.clusterScoped=true
```

## Provision a new Cassandra database

```text
helm repo add mt-fragtale https://mydriatech.github.io/fragtale
helm repo update
helm upgrade --install --atomic --create-namespace --namespace fragtale-demo --timeout 15m0s \
    --set size=2 \
    fragtale-k8cs charts/fragtale-k8cs
watch kubectl get -n fragtale-demo k8cs/fragtale-k8cs -o=jsonpath='{.status.datacenters.dc1.cassandra.cassandraOperatorProgress}'
# ...and be patient. This can take 5+ minutes to reach the "Ready" status depending on your hardware.
```

## Deploy Fragtale using the provisioned Cassandra database

```text
cat <<'EOF' > /tmp/fragtale-values.yaml
app:
  backend:
    cassandra:
      keyspace: fragtale
      credentials: fragtale-k8cs-superuser
      hosts:
      - fragtale-k8cs-dc1-custom-svc.fragtale-demo.svc:9042
      replicationFactor: 1
  integrity:
    generation: 0
ntp:
  enabled: true
EOF

helm repo add mt-fragtale https://mydriatech.github.io/fragtale
helm repo update
helm upgrade --install --atomic --create-namespace --namespace fragtale-demo \
    fragtale mt-fragtale/fragtale \
    -f /tmp/fragtale-values.yaml
```

## Invoke REST API using in-cluster authentication

Create a Pod in another namespace (`fragtale-demo-app`) and invoke REST API to get
a message published to topic `demo`.

```
kubectl create namespace fragtale-demo-app

cat <<'EOF' > /tmp/fragtale-demo-app.yaml
apiVersion: v1
kind: Pod
metadata:
  name: fragtale-client
  namespace: fragtale-demo-app
spec:
  containers:
  - name: fragtale-client
    image: alpine
    command: ["sh", "-c", "apk update; apk add curl ; sleep infinity"] 
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
  restartPolicy: Never
EOF

kubectl create -f /tmp/fragtale-demo-app.yaml
kubectl exec -it -n fragtale-demo-app fragtale-client -- sh

# Poll for events on topic "demo"
curl --header "Authorization: Bearer $(cat /var/run/secrets/tokens/service-account)" \
    http://fragtale.fragtale-demo.svc.cluster.local:8081/api/v1/topics/demo/next?from_epoch_ms=0 -o - -D -
#...this takes a since topic is created on the fly...

# Publish event to topic "demo"
curl --header "Authorization: Bearer $(cat /var/run/secrets/tokens/service-account)" \
    http://fragtale.fragtale-demo.svc.cluster.local:8081/api/v1/topics/demo/events -X PUT -D - --data '{"test":"this is an example event"}'

# Poll for events on topic "demo"
sleep 1
curl --header "Authorization: Bearer $(cat /var/run/secrets/tokens/service-account)" \
    http://fragtale.fragtale-demo.svc.cluster.local:8081/api/v1/topics/demo/next -o - -D -
logout

kubectl delete -f /tmp/fragtale-demo-app.yaml

```


## Where to go next

Start your scalable and resilient microservice development with the
[example](../fragtale-client/examples/event_handler.rs) using the provided Rust
[client library](../fragtale-client/) or check out the
[OpenAPI documentation](../fragtale-api/openapi.json) for a more language agnostic
approach.
