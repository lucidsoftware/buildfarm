// Copyright 2023 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <jni.h>
#include <stdlib.h>
#include <string.h>

#include "c/blake3.h"

namespace blaze_jni {

jbyte *get_byte_array(JNIEnv *env, jbyteArray java_array) {
  return (jbyte *)env->GetPrimitiveArrayCritical(java_array, nullptr);
}

void release_byte_array(JNIEnv *env, jbyteArray array, jbyte *addr) {
  env->ReleasePrimitiveArrayCritical(array, addr, 0);
}

extern "C" JNIEXPORT int JNICALL
Java_build_buildfarm_common_blake3_Blake3MessageDigest_hasher_1size(
    JNIEnv *env, jobject obj) {
  return (int)sizeof(blake3_hasher);
}

// Java byte arrays are not always properly aligned for a blake3_hasher, notably
// with compact object headers. Copy the state into aligned native storage.
extern "C" JNIEXPORT void JNICALL
Java_build_buildfarm_common_blake3_Blake3MessageDigest_initialize_1hasher(
    JNIEnv *env, jobject obj, jbyteArray jhasher) {
  blake3_hasher hasher;
  // Initialize padding as well as fields before copying native storage to Java.
  memset(&hasher, 0, sizeof(hasher));
  blake3_hasher_init(&hasher);
  env->SetByteArrayRegion(jhasher, 0, sizeof(hasher), (jbyte *)&hasher);
}

extern "C" JNIEXPORT void JNICALL
Java_build_buildfarm_common_blake3_Blake3MessageDigest_blake3_1hasher_1update(
    JNIEnv *env, jobject obj, jbyteArray jhasher, jbyteArray input, jint offset,
    jint input_len) {
  blake3_hasher hasher;
  env->GetByteArrayRegion(jhasher, 0, sizeof(hasher), (jbyte *)&hasher);
  if (env->ExceptionCheck()) {
    return;
  }
  jbyte *input_addr = get_byte_array(env, input);
  if (input_addr) {
    blake3_hasher_update(&hasher, input_addr + offset, input_len);
    release_byte_array(env, input, input_addr);
    env->SetByteArrayRegion(jhasher, 0, sizeof(hasher), (jbyte *)&hasher);
  }
}

extern "C" JNIEXPORT void JNICALL
Java_build_buildfarm_common_blake3_Blake3MessageDigest_blake3_1hasher_1finalize(
    JNIEnv *env, jobject obj, jbyteArray jhasher, jbyteArray out,
    jint out_len) {
  blake3_hasher hasher;
  env->GetByteArrayRegion(jhasher, 0, sizeof(hasher), (jbyte *)&hasher);
  if (env->ExceptionCheck()) {
    return;
  }
  jbyte *out_addr = get_byte_array(env, out);
  if (out_addr) {
    blake3_hasher_finalize(&hasher, (uint8_t *)out_addr, out_len);
    release_byte_array(env, out, out_addr);
  }
}

}  // namespace blaze_jni
