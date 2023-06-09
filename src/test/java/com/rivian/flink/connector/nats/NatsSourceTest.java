/*
 * Copyright 2023 Rivian Automotive, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rivian.flink.connector.nats;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.nats.client.Options;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by Pramod Immaneni <pimmaneni@rivian.com> on 5/31/23
 */
public class NatsSourceTest {

  @Test
  public void testOptionsSerializationWithKryo() throws IOException {
    Kryo kryo = new Kryo();

    /*
    Registration registration = kryo.register(Options.class);
    registration.setInstantiator(new ObjectInstantiator<Options>() {
      @Override
      public Options newInstance() {
        return Options.builder().build();
      }
    });
    */

    Options options = Options.builder().server("localhost:5000").build();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeObject(output, new NatsSource.OptionsConfig(options));
    output.close();

    Input input = new Input(new ByteArrayInputStream(baos.toByteArray()));
    Options roptions = kryo.readObject(input, NatsSource.OptionsConfig.class).options;
    input.close();
    Assert.assertNotNull(roptions);
    Assert.assertEquals("Number of servers", 1, roptions.getUnprocessedServers().size());
    Assert.assertEquals("Server", "localhost:5000", roptions.getUnprocessedServers().get(0));
  }

}
