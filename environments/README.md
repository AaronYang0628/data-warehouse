# 添加 ay-helm-mirror 仓库
```shell
helm repo add ay-helm-mirror https://aaronyang0628.github.io/helm-chart-mirror/charts
```

1. install metadata operator
```shell
### install from ay-mirror
helm upgrade  --create-namespace -n metadata --install -f /workspaces/data-warehouse/environments/helm/metadata-environment/values.yaml metadata ay-helm-mirror/data-warehouse  --version=0.0.9

# ## install from local
# helm upgrade  --create-namespace -n metadata --install -f /workspaces/data-warehouse/environments/helm/metadata-environment/values.yaml metadata /tmp/data-warehouse-0.0.9.tgz


# ## install to zverse
# helm upgrade  --kubeconfig=/root/.kube/zverse_config --create-namespace -n metadata --install -f /root/data-warehouse/environments/helm/metadata-environment/values.yaml metadata ay-helm-mirror/data-warehouse  --version=0.0.9

helm pull oci://harbor.zhejianglab.com/ay-dev/data-warehouse --version 0.0.25
helm upgrade  --create-namespace -n metadata --install -f /workspaces/data-warehouse/environments/helm/metadata-environment/values.yaml metadata ./data-warehouse-0.0.25.tgz
```