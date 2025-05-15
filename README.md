# Metadata Operator

### Local Develop
1. connect to your k8s or minikube
    * download `kubectl` binary [done by dockerfile]
    * create kubeconfig file, normally in `/root/.kube/config`


2. apply CRD
```shell
kubectl apply -f ./target/classes/META-INF/fabric8/metadataingesttasks.org.zhejianglab.astro-v1.yml
```

3. install CR
```shell
kubectl apply -f /workspaces/data-warehouse/templates/scan-oss-resource.yaml

kubectl -n metadata apply -f /workspaces/data-warehouse/templates/scan-s3-resource.yaml

```

### Maven CMD
0. init project
```shell
mvn io.javaoperatorsdk:bootstrapper:5.0.4:create -DprojectGroupId=org.zhejianglab.astro -DprojectArtifactId=data-warehouse
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