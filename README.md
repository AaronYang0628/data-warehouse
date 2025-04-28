# Metadata Operator

### Local Develop
1. connect to your k8s or minikube
    * download `kubectl` binary [done by dockerfile]
    * create kubeconfig file, normally in `/root/.kube/config`


2. apply CRD
```shell
kubectl apply -f ./target/classes/META-INF/fabric8/metadataoperatorcustomresources.org.zhejianglab.astro-v1.yml
```

3. install CR
```shell
kubectl apply -f ./k8s/test-resource.yaml
```

### Maven CMD
0. init project
```shell
mvn io.javaoperatorsdk:bootstrapper:5.0.4:create -DprojectGroupId=org.zhejianglab.astro -DprojectArtifactId=metadata-operator
```
1. format
```shell
mvn spotless:apply
```

2. check dependence
```shell
mvn dependency:tree
```

2. package jar
```shell
mvn clean package
```

2. build docker image
```shell
mvn jib:dockerBuild
```