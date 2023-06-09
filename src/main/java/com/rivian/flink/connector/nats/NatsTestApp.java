package com.rivian.flink.connector.nats;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Pramod Immaneni <pimmaneni@rivian.com> on 6/7/23
 */
public class NatsTestApp {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    NatsSource source = new NatsSource();
    source.setUrl("nats-main:4222");
    source.setSubject("test.subject");
    DataStream<String> stream = env.addSource(source);

    //PrintSinkFunction<String> sink = new PrintSinkFunction<>();
    LogSink sink = new LogSink();
    stream.addSink(sink).disableChaining();

    env.execute("NatsSourceTest");
  }

  public static class LogSink implements SinkFunction<String> {

    private static final Logger logger = LoggerFactory.getLogger(LogSink.class);

    @Override
    public void invoke(String value, SinkFunction.Context context) throws Exception {
      logger.info("Received data {}", value);
    }
  }
}
