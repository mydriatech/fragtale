# Fragtale metrics and Horizontal Pod Autoscaling

Metrics are provided at the `/metrics` end-point in the `PrometheusText0.0.4`
format and always enabled due to the low overhead of the selected values to
collect for scraping.

The Kubernetes [HorizontalPodAutoscaler](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
(HPA) allows additional instances to be started during high load and can be
enabled to automatically adapt to changing load.

## Extending the example deployment

This assumes that you have completed [the quick start guide](quick_start.md).

### Use Prometheus to scrape metrics

Install the metrics-server:

```
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
kubectl get deployment metrics-server -n kube-system
```

Deploy Prometheus with a scraping interval of 15 seconds:

```
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install --atomic prometheus prometheus-community/prometheus -n prometheus --create-namespace \
    --set server.global.scrape_interval=15s
```

Expose Fragtale's Custom Metrics to the HPA

```
cat <<'EOF' > /tmp/prometheus-adapter-values.yaml
prometheus:
  url: http://prometheus-server.prometheus.svc.cluster.local
  port: 80
rules:
  default: false
  custom:
    # Get published events per second.
    - seriesQuery: 'fragtale_mb_published_events_count{job="kubernetes-pods"}'
      resources:
        template: <<.Resource>>
      name:
        matches: "fragtale_mb_published_events_count"
        as: "fragtale_mb_published_events_per_second"
      metricsQuery: (sum by (<<.GroupBy>>) (rate(fragtale_mb_published_events_count{job="kubernetes-pods"}[2m])))
    # Get delivered events per second.
    - seriesQuery: 'fragtale_mb_delivered_events_count{job="kubernetes-pods"}'
      resources:
        template: <<.Resource>>
      name:
        matches: "fragtale_mb_delivered_events_count"
        as: "fragtale_mb_delivered_events_per_second"
      metricsQuery: (sum by (<<.GroupBy>>) (rate(fragtale_mb_delivered_events_count{job="kubernetes-pods"}[2m])))
EOF

helm upgrade --install --atomic prometheus-adapter prometheus-community/prometheus-adapter -n prometheus --create-namespace \
    -f /tmp/prometheus-adapter-values.yaml
```

Add the following to your Helm chart's `/tmp/fragtale-values.yaml` override file
and update Fragtale to start scraping metrics and scaling on the custom metrics:

```text
cat <<'EOF' >> /tmp/fragtale-values.yaml
podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "8081"
  prometheus.io/path: "/metrics"

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 5
  #targetCPUUtilizationPercentage: 80
  #targetMemoryUtilizationPercentage: 80
  additionalMetrics:
  - type: Pods
    pods:
      metric:
        name: fragtale_mb_published_events_per_second
      target:
        type: AverageValue
        averageValue: 50
  - type: Pods
    pods:
      metric:
        name: fragtale_mb_delivered_events_per_second
      target:
        type: AverageValue
        averageValue: 50
EOF

helm upgrade --install --atomic --create-namespace --namespace fragtale-demo \
    fragtale mt-fragtale/fragtale \
    -f /tmp/fragtale-values.yaml
```

Once you start having some load on Fragtale, the custom metrics can be show using:

```text
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/fragtale-demo/pods/*/fragtale_mb_published_events_per_second" | jq
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/fragtale-demo/pods/*/fragtale_mb_delivered_events_per_second" | jq
```


### (Optional) Adding a Grafana dashboard to view Fragtale metrics

Install Grafana at `http://grafana.localdomain` with default credentials `admin:foo123`:

```
cat <<'EOF' > /tmp/grafana-values.yaml

adminPassword: "foo123"

persistence:
  enabled: true
  size: 10Gi
  #storageClassName: standard

datasources:
  datasources.yaml:
    apiVersion: 1
    datasources:
    - name: Prometheus
      type: prometheus
      url: http://prometheus-server.prometheus.svc.cluster.local
      access: proxy
      isDefault: true

dashboardProviders:
  dashboardproviders.yaml:
    apiVersion: 1
    providers:
    - name: 'default'
      orgId: 1
      folder: ''
      type: file
      disableDeletion: false
      editable: true
      options:
        path: /var/lib/grafana/dashboards/default

dashboards:
  default:
    kubernetes:
      gnetId: 15661
      revision: 1
      datasource: Prometheus
    node-exporter:
      gnetId: 1860
      revision: 27
      datasource: Prometheus
EOF

helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
helm upgrade --install --atomic grafana grafana/grafana -n grafana --create-namespace \
    -f /tmp/grafana-values.yaml \
    --set ingress.enabled=true \
    --set ingress.hosts={grafana.localdomain}
```

Once logged in you can import the example dashboard [grafana_dashboard_Fragtale.json](metrics_and_auto_scaling/grafana_dashboard_Fragtale.json).
