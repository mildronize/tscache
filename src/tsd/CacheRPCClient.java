package net.opentsdb.tsd;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * Created by mildronize on 4/2/2017.
 */

// This class is just a simple RPCClient with blocking I/O.

public class CacheRPCClient {
  private Connection connection;
  private Channel channel;
  private String requestQueueName;
  private String replyQueueName;

  public CacheRPCClient(final String host, final String requestQueueName) throws IOException, TimeoutException {
    this.requestQueueName = requestQueueName;
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(host);

    connection = factory.newConnection();
    channel = connection.createChannel();

    replyQueueName = channel.queueDeclare().getQueue();
  }

  public String call(String message) throws IOException, InterruptedException {
    final String corrId = UUID.randomUUID().toString();

    AMQP.BasicProperties props = new AMQP.BasicProperties
      .Builder()
      .correlationId(corrId)
      .replyTo(replyQueueName)
      .build();

    channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

    final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);

    channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        if (properties.getCorrelationId().equals(corrId)) {
          response.offer(new String(body, "UTF-8"));
        }
      }
    });

    return response.take();
  }

  public void close() throws IOException {
    connection.close();
  }

}
