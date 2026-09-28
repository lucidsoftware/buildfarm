load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "tests",
    size = "small",
    srcs = glob(["*.java"], exclude = ["PersistentWorkerCgroupIntegrationTest.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common/base",
        "//src/main/java/build/buildfarm/worker/cgroup",
        "//src/test/java/build/buildfarm:test_runner",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:org_mockito_mockito_core",
        "@buildfarm_maven//:org_projectlombok_lombok",
    ],
)

java_test(
    name = "PersistentWorkerCgroupIntegrationTest",
    srcs = ["PersistentWorkerCgroupIntegrationTest.java"],
    test_class = "build.buildfarm.worker.cgroup.PersistentWorkerCgroupIntegrationTest",
    tags = ["manual", "integration", "local"],
    resources = ["//persistentworkers/examples/src/main/java:adder-bin_deploy.jar"],
    deps = [
        "//src/main/java/build/buildfarm/worker/cgroup",
        "//persistentworkers/src/main/java/persistent/bazel:bazel-persistent-workers",
        "//persistentworkers/src/main/java/persistent/common:persistent-common",
        "//persistentworkers/src/main/protobuf:worker_protocol_java_proto",
        "//persistentworkers/src/test/java/persistent/testutil:testutil",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:org_apache_commons_commons_pool2",
    ],
)
