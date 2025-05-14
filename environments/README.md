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
1. install cert 
```shell
helm upgrade  --create-namespace -n <$namespace> --install -f ./values.yaml metadata-env ay-helm-mirror/ingress-nginx --version=4.11.3

helm upgrade  --create-namespace -n metadata --install -f /workspaces/data-warehouse/environments/helm/metadata-environment/values.yaml metadata /tmp/data-warehouse-0.0.5.tgz

```