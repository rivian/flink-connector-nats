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

import com.esotericsoftware.kryo.DefaultSerializer;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.Subscription;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Created by Pramod Immaneni <pimmaneni@rivian.com> on 5/31/23
 */
public class NatsSource extends RichParallelSourceFunction<String> {

  private static final Logger logger = LoggerFactory.getLogger(NatsSource.class);

  String url;

  OptionsConfig optionsConfig;

  @Nonnull
  String subject;

  transient Connection connection;
  volatile transient boolean stop;

  @Override
  public void open(Configuration parameters) throws Exception {
    if (url != null) {
      connection = Nats.connect(url);
    } else if (optionsConfig != null) {
      connection = Nats.connect(optionsConfig.options);
    } else {
      connection = Nats.connect();
    }
  }

  @Override
  public void run(SourceContext<String> sourceContext) throws Exception {
    Subscription subscription = connection.subscribe(subject);
    while (!stop) {
      Message msg = subscription.nextMessage(Duration.ofMillis(500));
      if (msg != null) {
        sourceContext.collect(new String(msg.getData(), StandardCharsets.UTF_8));
      }
    }
  }

  @Override
  public void cancel() {
    try {
      stop = true;
      connection.close();
    } catch (InterruptedException e) {
      logger.warn("Error closing connection", e);
    }
  }

  @DefaultSerializer(OptionsSerializer.class)
  static class OptionsConfig {
    Options options;

    public OptionsConfig() {
    }

    public OptionsConfig(Options options) {
      this.options = options;
    }
  }

  @Nonnull
  public String getUrl() {
    return url;
  }

  public void setUrl(@Nonnull String url) {
    this.url = url;
  }

  public Options getOptions() {
    return optionsConfig != null ? optionsConfig.options : null;
  }

  public void setOptions(Options options) {
    optionsConfig = new OptionsConfig(options);
  }

  @Nonnull
  public String getSubject() {
    return subject;
  }

  public void setSubject(@Nonnull String subject) {
    this.subject = subject;
  }
}
