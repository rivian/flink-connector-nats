package com.rivian.flink.connector.nats;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
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

  @Nonnull
  String url;

  @Nonnull
  String subject;

  transient Connection connection;
  volatile transient boolean stop;

  @Override
  public void open(Configuration parameters) throws Exception {
    connection = Nats.connect(url);
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

  @Nonnull
  public String getUrl() {
    return url;
  }

  public void setUrl(@Nonnull String url) {
    this.url = url;
  }

  @Nonnull
  public String getSubject() {
    return subject;
  }

  public void setSubject(@Nonnull String subject) {
    this.subject = subject;
  }
}
