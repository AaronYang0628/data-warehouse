# 添加 Bitnami 仓库
```shell
helm repo add ay-helm-mirror https://aaronyang0628.github.io/helm-chart-mirror/charts
```

# package chart
```shell
helm package --destination /tmp/ /workspaces/data-warehouse/environments/helm/metadata-environment
```

# install chart

0. install cert-manager [optional]
```shell
kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml
```

1. install flink-operator [optional]
```shell
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.11.0/
helm install --create-namespace -n flink flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
```

2. install metadata operator
```shell
## install from local
helm upgrade  --create-namespace -n metadata --install -f /workspaces/data-warehouse/environments/helm/metadata-environment/values.yaml metadata /tmp/data-warehouse-0.0.9.tgz

### install from ay-mirror
helm upgrade  --create-namespace -n metadata --install -f /workspaces/data-warehouse/environments/helm/metadata-environment/values.yaml metadata ay-helm-mirror/data-warehouse  --version=0.0.9

## install to zverse
helm upgrade  --kubeconfig=/root/.kube/zverse_config --create-namespace -n metadata --install -f /root/data-warehouse/environments/helm/metadata-environment/values.yaml metadata ay-helm-mirror/data-warehouse  --version=0.0.9
```