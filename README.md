# Metadata Operator
- Deploy Flink + ES + Kafka middleware in one shot
- Create Operator to manage metadata ingest job

### Local Develop
1. connect to your k8s or minikube
    * download `kubectl` binary [done by dockerfile]
    * create kubeconfig file, normally in `/root/.kube/config`

2. init develop environment
you can follow the `environments/README.md`  to init develop environment


### Apply CRD
```shell
### flink job
kubectl apply -f environments/helm/metadata-environment/crds/flinkingesttasks.org.zhejianglab.astro.metadata-v1.yaml

### java crud
kubectl apply -f  environments/helm/metadata-environment/crds/virtualingesttasks.org.zhejianglab.astro.metadata-v1.yaml
```

### Install Metadata Operator
```shell
### install from ay-mirror
helm upgrade  --create-namespace -n metadata --install -f /workspaces/data-warehouse/environments/helm/metadata-environment/values.yaml metadata ay-helm-mirror/data-warehouse  --version=0.0.9
```

### Submit CR job
```shell
### flink job
kubectl -n metadata apply -f templates/scan-oss-resource.yaml
kubectl -n metadata apply -f templates/scan-s3-resource.yaml

### java crud
kubectl -n metadata apply -f templates/scan-virtual-resource.yaml

```

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

3. package jar
```shell
mvn clean package
```

4. build docker image
```shell
docker login -u <username> -p <password>
mvn compile jib:build
```

