# 添加 ay-helm-mirror 仓库
```shell
helm repo add ay-helm-mirror https://aaronyang0628.github.io/helm-chart-mirror/charts
```

# [[Optional]]() verify chart
```shell
helm lint /workspaces/data-warehouse/environments/helm/metadata-environment
# helm lint environments/helm/metadata-environment
```

# [[Optional]]() package chart
```shell
helm package --dependency-update  --destination /tmp/ /workspaces/data-warehouse/environments/helm/metadata-environment
# helm package --destination /tmp/ /workspaces/data-warehouse/environments/helm/metadata-environment
# helm package --dependency-update  --destination /tmp/ environments/helm/metadata-environment
# helm package  --destination /tmp/ environments/helm/metadata-environment
```

### [[Optional]]() Apply CRD
```shell
### flink job
kubectl apply -f environments/helm/metadata-environment/crds/flinkingesttasks.org.zhejianglab.astro.metadata-v1.yaml

### java crud
kubectl apply -f  environments/helm/metadata-environment/crds/croningesttasks.org.zhejianglab.astro.metadata-v1.yaml
```

# install chart

0. [[Optional]]() install cert-manager
```shell
kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.17.2/cert-manager.yaml
```

1. [[Optional]]() install flink-operator 
```shell
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.13.0/
helm install --create-namespace -n flink flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
```

2. test metadata operator
```shell
## install from local
helm upgrade  --create-namespace -n warehouse --install -f /workspaces/data-warehouse/environments/helm/metadata-environment/dev.yaml warehouse /tmp/data-warehouse-0.0.24.tgz

# ## install to zverse
# helm upgrade  --kubeconfig=/root/.kube/zverse_config --create-namespace -n metadata --install -f /root/data-warehouse/environments/helm/metadata-environment/values.yaml metadata ay-helm-mirror/data-warehouse  --version=0.0.10
```



### Kafka

```shell
kubectl -n warehouse exec $(kubectl -n warehouse get pods -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}') -- sh -c "kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ingest-to-es && kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ingest-to-es --partitions 16 --replication-factor 1"
```

```
kubectl -n warehouse exec -it warehouse-kafka-controller-0 -- kafka-console-consumer.sh   --bootstrap-server localhost:9092   --topic ingest-to-es 
```