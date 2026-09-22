"""ARM linker adapter for Rust and hermetic_cc_toolchain's Zig driver."""

load("@bazel_tools//tools/build_defs/cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")

ArmCcInfo = provider(fields = ["files", "linker"])

def _arm_cc_info_impl(ctx):
    cc_toolchain = ctx.toolchains["@bazel_tools//tools/cpp:toolchain_type"].cc
    features = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc_toolchain,
    )
    return [ArmCcInfo(
        files = cc_toolchain.all_files,
        linker = cc_common.get_tool_for_action(
            action_name = ACTION_NAMES.cpp_link_executable,
            feature_configuration = features,
        ),
    )]

arm_cc_info = rule(
    implementation = _arm_cc_info_impl,
    fragments = ["cpp"],
    toolchains = ["@bazel_tools//tools/cpp:toolchain_type"],
)

def _arm_transition_impl(_settings, _attr):
    return {"//command_line_option:platforms": ["@zig_sdk//platform:linux_arm64"]}

_arm_transition = transition(
    implementation = _arm_transition_impl,
    inputs = [],
    outputs = ["//command_line_option:platforms"],
)

def _linker_impl(ctx):
    arm = ctx.attr.arm[0][ArmCcInfo]
    script = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.write(
        output = script,
        content = """#!/bin/bash
args=()
for arg in "$@"; do
  if [[ "$arg" != "-Wl,--fix-cortex-a53-843419" ]]; then
    args+=("$arg")
  fi
done
exec "{linker}" "${{args[@]}}"
""".format(linker = arm.linker),
        is_executable = True,
    )
    return [DefaultInfo(
        executable = script,
        runfiles = ctx.runfiles(transitive_files = arm.files),
    )]

rust_arm_linker = rule(
    implementation = _linker_impl,
    attrs = {
        "arm": attr.label(
            cfg = _arm_transition,
            providers = [ArmCcInfo],
        ),
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
    executable = True,
)
