package com.rivian.flink.connector.nats;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.nats.client.Options;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Pramod Immaneni <pimmaneni@rivian.com> on 6/8/23
 */
public class OptionsSerializer extends Serializer<NatsSource.OptionsConfig> {

  @Override
  public void write(Kryo kryo, Output output, NatsSource.OptionsConfig object) {
    Options options = object.options;
    kryo.writeClassAndObject(output, options.getUnprocessedServers());
    output.writeBoolean(options.isNoRandomize());
    output.writeBoolean(options.isNoResolveHostnames());
    output.writeBoolean(options.isReportNoResponders());
    output.writeString(options.getConnectionName());
  }

  @Override
  public NatsSource.OptionsConfig read(Kryo kryo, Input input, Class<NatsSource.OptionsConfig> type) {
    try {
      Options.Builder builder = Options.builder();
      List<String> serverList = (List<String>)kryo.readClassAndObject(input);
      builder.servers(serverList.toArray(new String[0]));
      if (input.readBoolean()) builder.noRandomize();
      if (input.readBoolean()) builder.noResolveHostnames();
      if (input.readBoolean()) builder.reportNoResponders();
      builder.connectionName(input.readString());
      Options options =  builder.build();
      return new NatsSource.OptionsConfig(options);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
