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
