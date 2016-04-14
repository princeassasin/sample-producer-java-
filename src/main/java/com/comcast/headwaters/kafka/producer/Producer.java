package com.comcast.headwaters.kafka.producer;

import java.net.InetAddress;
import java.net.UnknownHostException;
//import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import headwaters.CoreHeader;
import headwaters.core.MonitoringEvent;
import headwaters.datatype.Timestamp;
import headwaters.datatype.UUID;

/**
 * This is an example of the Headwaters data producer code. It is kept to its most simple form in order to demonstrate
 * the core feature of the program. For instance, we hard-coded the configuration settings (which obviously is not
 * ideal). <br />
 * It uses as an example the {@link MonitoringEvent}, chosen here again for simplicity. But it works the same way for
 * any schema defined in Headwaters.
 */
public class Producer {
  private static final Logger LOG = LoggerFactory.getLogger(Producer.class);
  private KafkaProducer<String, MonitoringEvent> producer;
  public final String topic;
  public final String hostname;
  public final String monitoringId;
  
  /**
   * Builds and configure the Kafka Producer
   * 
   * @param topic
   *          The name of the topic to produce data to
   */
  public Producer(String topic) {
    this.topic = topic;
    String thisHostname = "";
    try {
      thisHostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      thisHostname = "unknown";
    }
    hostname = thisHostname;
    monitoringId = "LabWeekDemo";
    // Creating the Kafka producer configuration
    Properties props = new Properties();
    props.put("bootstrap.servers", "headwaters-producer.sandbox.video.comcast.net:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "com.comcast.headwaters.kafka.producer.AvroSerializer");
    // Creating the actual Kafka producer
    producer = new KafkaProducer<>(props);
    LOG.debug("Producer configured for topic: {}", topic);
  }

  /**
   * Starts the producer, which emits a new Monitoring event every second. The value conveyed in the monitoring event is
   * the time (in microseconds) it took to send the previous monitoring event.
   */
  public void start() {
    LOG.info("Starting Producer");
    long sendPreviousTime = 0;
    while (true) {
      try {
        final long startTime = System.nanoTime();
        // Sending the data to Kafka. The Kafka record contains the topic, the key and the value, using the type defined
        // in the configuration in the constructor above. So in this case, the key is a String, and the value a
        // MonitoringEvent.
        producer.send(new ProducerRecord<>(topic, String.valueOf(System.currentTimeMillis()), createMonitoringEvent(sendPreviousTime)));
        final long endTime = System.nanoTime();
        sendPreviousTime = (endTime - startTime) / 1000;
        LOG.debug("Sent message in " + sendPreviousTime + " microseconds");
        Thread.sleep(1000);
      } catch (Exception e) {
        sendPreviousTime = -1;
        LOG.warn("Error while sending monitoring event: ", e);
      }
    }
  }

  /**
   * Stops the producer
   */
  public void stop() {
    try {
      producer.close();
    } catch (Exception e) {
      LOG.warn("Failed to close kafka producer", e);
    }
  }

  /**
   * Creates a new monitoring event
   * 
   * @param totalStartime
   *          The context value in this case is the time (in microseconds) it took to send the previous message
   * @return The newly created monitoring event
   */
  public MonitoringEvent createMonitoringEvent(final long totalStartime) {
    final CoreHeader header = new CoreHeader();
    header.setHostname(hostname);
    header.setTimestamp(new Timestamp(System.currentTimeMillis()));
    header.setUuid(new UUID(java.util.UUID.randomUUID().toString()));
    final MonitoringEvent event = new MonitoringEvent();
    event.setMonitoringEventId(monitoringId);
    event.setHeader(header);
    event.setContextValue(totalStartime);
    return event;
  }

  /**
   * This is the entry point to run the producer code.
   * 
   * @param args
   *          Program argument (ignored)
   */
  public static void main(String[] args) {
    final Producer producer = new Producer("clean.headwaters.core.MonitoringEvent");
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void start() {
        producer.stop();
      }
    });
    producer.start();
  }
}
