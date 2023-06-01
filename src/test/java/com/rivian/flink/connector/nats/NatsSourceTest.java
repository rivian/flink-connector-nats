package com.rivian.flink.connector.nats;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

/**
 * Created by Pramod Immaneni <pimmaneni@rivian.com> on 5/31/23
 */
public class NatsSourceTest {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    NatsSource source = new NatsSource();
    source.setUrl("localhost:4222");
    source.setSubject("test.subject");
    DataStream<String> stream = env.addSource(source);

    PrintSinkFunction<String> sink = new PrintSinkFunction<>();
    stream.addSink(sink);

    env.execute("NatsSourceTest");
  }
}
