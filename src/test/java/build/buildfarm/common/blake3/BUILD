load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "tests",
    size = "small",
    srcs = ["Blake3MessageDigestTest.java"],
    jvm_flags = ["-Xcheck:jni"],
    test_class = "build.buildfarm.common.blake3.Blake3MessageDigestTest",
    deps = [
        "//src/main/java/build/buildfarm/common/blake3",
        "@buildfarm_maven//:junit_junit",
    ],
)
