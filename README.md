# Metadata Operator
- Deploy Cert Manager + Flink + ES + Kafka + API server middleware in one shot
- Create Operator to manage metadata ingest job


### [[Optional]]() Apply CRD
```shell
### flink job
kubectl apply -f environments/helm/metadata-environment/crds/flinkingesttasks.org.zhejianglab.astro.metadata-v1.yaml

### cron job
kubectl apply -f environments/helm/metadata-environment/crds/croningesttasks.org.zhejianglab.astro.metadata-v1.yaml

```

### Install Metadata Operator
```shell
helm repo add ay-helm-mirror https://aaronyang0628.github.io/helm-chart-mirror/charts
wget -O metadata.values.yaml https://raw.githubusercontent.com/AaronYang0628/helm-chart-mirror/refs/heads/main/charts/data-and-computing/data.warehouse.values.yaml
### install from ay-mirror
helm upgrade  --create-namespace -n warehouse --install -f metadata.values.yaml data-warehouse ay-helm-mirror/data-warehouse  --version=0.0.14
```

### Submit Data Ingest job
```shell
### flink job
kubectl -n metadata apply -f templates/scan-s3-resource.big.yaml
kubectl -n metadata apply -f templates/scan-s3-resource.medium.yaml
kubectl -n metadata apply -f templates/scan-s3-resource.small.yaml

# kubectl --kubeconfig=/root/.kube/zverse_config get -n metadata apply -f templates/scan-s3-resource.long.yaml
# kubectl --kubeconfig=/root/.kube/zverse_config get -n metadata apply -f templates/scan-s3-resource.short.yaml

### java crud
kubectl -n metadata apply -f templates/scan-virtual-resource.yaml

# kubectl --kubeconfig=/root/.kube/zverse_config get -n metadata apply -f templates/scan-virtual-resource.yaml
```

### Check Job Status
```shell
kubectl get flinkingest -A
kubectl get virtualingest -A
```


### Local Develop
1. connect to your k8s or minikube
    * download `kubectl` binary [done by dockerfile]
    * create kubeconfig file, normally in `/root/.kube/config`

2. init develop environment
you can follow the `environments/README.md`  to init develop environment

### Maven CMD
0. init project
```shell
mvn io.javaoperatorsdk:bootstrapper:5.0.4:create -DprojectGroupId=org.zhejianglab.astro.metadata -DprojectArtifactId=data-warehouse
```
1. format
```shell
mvn spotless:apply
```

2. check dependence
```shell
mvn dependency:tree
```

3. package jar (also generate new CRD)
```shell
#/workspaces/data-warehouse/target/classes/META-INF/fabric8
mvn clean package
```

4. build docker image
```shell
export DOCKER_CR_PAT=dckr_pat_bBN_Xkgz-TRdxirM2B6EDYCjjrg
echo $DOCKER_CR_PAT | podman login docker.io -u aaron666 --password-stdin
mvn compile jib:build
```

### TODO
- some resources cannot remove, even though there are
```shell
[root@ay-zj-ecs data-warehouse]# kubectl -n metadata logs -f ingest-operator-flink-post-cleanup-job-td9gs
CRD 'flinkingesttasks.org.zhejianglab.astro.metadata' not found
CRD 'virtualingesttasks.org.zhejianglab.astro.metadata' not found
In Namespace ayyy Secret 'metadata-minio-secret' not found
In Namespace ayyy ServiceAccount 'flink' not found
In Namespace ayyy  Role 'flink' not found
In Namespace ayyy  RoleBinding 'flink-role-binding' not found
Cleanup completed successfully
[root@ay-zj-ecs data-warehouse]# kubectl -n metadata logs -f ingest-operator-flink-pre-cleanup-job-b9tbg
In Namespace ayyy Flinkdeployment 'metadata-flink-job-ingest-kafka-to-es' not found
In Namespace ayyy, there is no SessionJob hosted by metadata-flink-session-cluster
In Namespace ayyy Flinkdeployment 'metadata-flink-session-cluster' not found
Cleanup completed successfully
```