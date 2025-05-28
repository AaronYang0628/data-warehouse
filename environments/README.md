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
1. install metadata 
```shell
## install from local
helm upgrade  --create-namespace -n metadata --install -f /workspaces/data-warehouse/environments/helm/metadata-environment/values.yaml metadata /tmp/data-warehouse-0.0.6.tgz

### install from ay-mirror
helm upgrade  --create-namespace -n metadata --install -f /workspaces/data-warehouse/environments/helm/metadata-environment/values.yaml metadata ay-helm-mirror/data-warehouse  --version=0.0.6
```