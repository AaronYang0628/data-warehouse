package org.acme;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.javaoperatorsdk.operator.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Runner {

    private static final Logger log = LoggerFactory.getLogger(Runner.class);

    public static void main(String[] args) {

        Operator operator = new Operator(overrider -> {
            overrider.withKubernetesClient(
                new KubernetesClientBuilder()
                .build()
                );
        });
        operator.register(new MetadataOperatorReconciler());
        log.info("Operator started.");
        operator.start();
    }
}
