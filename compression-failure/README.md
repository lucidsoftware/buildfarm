
This folder holds the repro case for a data corruption failure when using `--remote_cache_compression`

Steps to reproduce:
1. bazel run //src/main/java/build/buildfarm:buildfarm-server -- --jvm_flag=-Djava.util.logging.config.file=$PWD/examples/logging.properties $PWD/examples/config.minimal.yml
2. bazel run //src/main/java/build/buildfarm:buildfarm-shard-worker -- --jvm_flag=-Djava.util.logging.config.file=$PWD/examples/logging.properties $PWD/examples/config.minimal.yml
3. bazel build //compression-failure --remote_executor=grpc://localhost:8980 --strategy=remote --remote_cache_compression
