# 添加 Bitnami 仓库
```shell
helm repo add ay-helm-mirror https://aaronyang0628.github.io/helm-chart-mirror/charts
```

# package chart
```shell
helm package --dependency-update --destination /tmp/ /root/metadata-operator/environments/helm/metadata-environment/
```

# install chart
```shell
helm upgrade  --create-namespace -n <$namespace> --install -f ./values.yaml metadata-env ay-helm-mirror/ingress-nginx --version=4.11.3

helm upgrade  --create-namespace -n test --install -f ./values.yaml metadata-env /tmp/metadata-environment-0.0.1.tgz

```