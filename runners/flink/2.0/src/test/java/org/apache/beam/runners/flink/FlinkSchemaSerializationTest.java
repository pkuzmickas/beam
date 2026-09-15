/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Regression coverage for Schema serialization with an isolated user-code Kryo dependency. */
@RunWith(JUnit4.class)
public class FlinkSchemaSerializationTest {
  @Test
  public void schemaSerializerUsesFlinkClassLoader() throws Exception {
    ClassLoader parent = getClass().getClassLoader();
    String environmentsName = FlinkExecutionEnvironments.class.getName();
    URL[] urls = {
      FlinkExecutionEnvironments.class.getProtectionDomain().getCodeSource().getLocation(),
      Kryo.class.getProtectionDomain().getCodeSource().getLocation()
    };
    // Only the environment factory and Kryo are isolated. Sharing the remaining Beam classes keeps
    // this regression independent of the test runner's packaging and dependency layout.
    try (URLClassLoader userCodeLoader =
        new URLClassLoader(urls, parent) {
          @Override
          protected synchronized Class<?> loadClass(String name, boolean resolve)
              throws ClassNotFoundException {
            if (name.startsWith(environmentsName)
                || name.startsWith("com.esotericsoftware.kryo.")) {
              Class<?> loaded = findLoadedClass(name);
              if (loaded == null) {
                loaded = findClass(name);
              }
              if (resolve) {
                resolveClass(loaded);
              }
              return loaded;
            }
            return super.loadClass(name, resolve);
          }
        }) {
      assertNotSame(Kryo.class, userCodeLoader.loadClass(Kryo.class.getName()));
      Class<?> environments = userCodeLoader.loadClass(environmentsName);
      assertSame(userCodeLoader, environments.getClassLoader());
      FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
      options.setFlinkMaster("[local]");
      StreamExecutionEnvironment environment =
          (StreamExecutionEnvironment)
              environments
                  .getMethod(
                      "createStreamExecutionEnvironment",
                      FlinkPipelineOptions.class,
                      List.class,
                      String.class)
                  .invoke(null, options, Collections.emptyList(), null);
      SerializerConfigImpl config =
          InstantiationUtil.deserializeObject(
              InstantiationUtil.serializeObject(environment.getConfig().getSerializerConfig()),
              userCodeLoader);
      Schema nested = Schema.builder().addStringField("name").build();
      Schema schema = Schema.builder().addRowField("nested", nested).addInt64Field("count").build();
      KryoSerializer<Schema> serializer = new KryoSerializer<>(Schema.class, config);
      DataOutputSerializer output = new DataOutputSerializer(128);
      serializer.serialize(schema, output);
      assertEquals(
          schema, serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer())));
      assertSame(
          KryoSerializer.class.getClassLoader(),
          config.getRegisteredTypesWithKryoSerializerClasses().get(Schema.class).getClassLoader());
    }
  }

  @Test
  public void schemaSerializationRemainsCompatible() throws Exception {
    SerializerConfigImpl oldConfig = new SerializerConfigImpl();
    oldConfig.registerTypeWithKryoSerializer(Schema.class, JavaSerializer.class);
    KryoSerializer<Schema> oldSerializer = new KryoSerializer<>(Schema.class, oldConfig);
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setFlinkMaster("[local]");
    StreamExecutionEnvironment environment =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(options);
    KryoSerializer<Schema> serializer =
        new KryoSerializer<>(Schema.class, environment.getConfig().getSerializerConfig());
    Schema schema =
        Schema.builder()
            .addRowField("nested", Schema.builder().addStringField("name").build())
            .addInt64Field("count")
            .build();
    DataOutputSerializer oldOutput = new DataOutputSerializer(128);
    DataOutputSerializer output = new DataOutputSerializer(128);
    oldSerializer.serialize(schema, oldOutput);
    serializer.serialize(schema, output);
    assertArrayEquals(oldOutput.getCopyOfBuffer(), output.getCopyOfBuffer());
    assertEquals(
        schema, serializer.deserialize(new DataInputDeserializer(oldOutput.getCopyOfBuffer())));
    assertEquals(
        schema, oldSerializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer())));

    DataOutputSerializer snapshotOutput = new DataOutputSerializer(128);
    TypeSerializerSnapshot.writeVersionedSnapshot(
        snapshotOutput, oldSerializer.snapshotConfiguration());
    TypeSerializerSnapshot<Schema> restoredSnapshot =
        TypeSerializerSnapshot.readVersionedSnapshot(
            new DataInputDeserializer(snapshotOutput.getCopyOfBuffer()),
            getClass().getClassLoader());
    assertTrue(
        serializer
            .snapshotConfiguration()
            .resolveSchemaCompatibility(restoredSnapshot)
            .isCompatibleAsIs());
  }
}
