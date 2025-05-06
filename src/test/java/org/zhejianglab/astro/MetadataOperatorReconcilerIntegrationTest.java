// package org.zhejianglab.astro;

// import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
// import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;
// import java.util.List;
// import java.util.Map;
// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.extension.RegisterExtension;
// import org.zhejianglab.astro.customresource.MetadataIngestTask;
// import org.zhejianglab.astro.customresource.MetadataIngestTaskSpec;
// import org.zhejianglab.astro.customresource.ScanConfig;
// import org.zhejianglab.astro.reconciler.MetadataOperatorDefaultReconciler;

// class MetadataOperatorReconcilerIntegrationTest {

//   public static final String RESOURCE_NAME = "test1";
//   public static final String INITIAL_VALUE = "initial value";
//   public static final String CHANGED_VALUE = "changed value";

//   @RegisterExtension
//   LocallyRunOperatorExtension extension =
//       LocallyRunOperatorExtension.builder()
//           .withReconciler(MetadataOperatorDefaultReconciler.class)
//           .build();

//   @Test
//   void testCRUDOperations() {
//     var cr = extension.create(testResource());

//     // await()
//     //     .untilAsserted(
//     //         () -> {
//     //           var cm = extension.get(ConfigMap.class, RESOURCE_NAME);
//     //           assertThat(cm).isNotNull();
//     //           assertThat(cm.getData()).containsEntry(KEY, INITIAL_VALUE);
//     //         });

//     // cr.getSpec().setPath
//     // cr = extension.replace(cr);

//     // await()
//     //     .untilAsserted(
//     //         () -> {
//     //           var cm = extension.get(ConfigMap.class, RESOURCE_NAME);
//     //           assertThat(cm.getData()).containsEntry(KEY, CHANGED_VALUE);
//     //         });

//     // extension.delete(cr);

//     // await()
//     //     .untilAsserted(
//     //         () -> {
//     //           var cm = extension.get(ConfigMap.class, RESOURCE_NAME);
//     //           assertThat(cm).isNull();
//     //         });
//   }

//   MetadataIngestTask testResource() {
//     var resource = new MetadataIngestTask();
//     resource.setMetadata(new ObjectMetaBuilder().withName(RESOURCE_NAME).build());
//     resource.setSpec(
//         MetadataIngestTaskSpec.builder()
//             .path("path")
//             .platform("s3")
//             .timeout(20)
//             .extraSecret("empty")
//             .scanConfig(
//                 ScanConfig.builder()
//                     .tags(List.of("a"))
//                     .userProperties(Map.of("a", "b"))
//                     .pathPatterns(Map.of("c", "d"))
//                     .allowedSuffixes(List.of("e"))
//                     .build())
//             .build());
//     return resource;
//   }
// }
