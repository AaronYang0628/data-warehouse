### Install
```shell
helm --kubeconfig=/root/.kube/zverse_config upgrade  --create-namespace -n datawarehouse --install -f /root/data-warehouse/environments/ops/zverse/values.yaml datawarehouse ay-helm-mirror/data-warehouse  --version=0.0.9
```

### Uninstall
```shell
helm --kubeconfig=/root/.kube/zverse_config  -n datawarehouse uninstall  datawarehouse
```


### Submit Job
```shell
kubectl --kubeconfig=/root/.kube/zverse_config -n datawarehouse apply -f /root/data-warehouse/environments/ops/zverse/jobs/scan-oss-resource.yaml

kubectl --kubeconfig=/root/.kube/zverse_config -n datawarehouse apply -f /root/data-warehouse/environments/ops/zverse/jobs/scan-s3-resource.yaml

kubectl --kubeconfig=/root/.kube/zverse_config -n datawarehouse apply -f /root/data-warehouse/environments/ops/zverse/jobs/scan-virtual-resource.yaml
```