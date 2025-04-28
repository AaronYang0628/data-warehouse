# Metadata Operator

### Local Develop
1. connect to your k8s or minikube
    * download `kubectl` binary [done by dockerfile]
    * create kubeconfig file, normally in `/root/.kube/config`


2. apply CRD
```shell
kubectl apply -f ./metadata-operator/target/classes/META-INF/fabric8/metadataoperatorcustomresources.org.acme-v1.yml
```

3. install CR
```shell
kubectl apply -f ./metadata-operator/k8s/test-resource.yaml
```

### Maven CMD
1. format
```shell
mvn spotless:apply
```

2. package jar
```shell
mvn clean package
```

2. build docker image
```shell
mvn jib:dockerBuild
```