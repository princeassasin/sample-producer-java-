package com.comcast.headwaters.kafka.producer;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.headwaters.serde.AvroSerDe;

import headwaters.core.MonitoringEvent;

/**
 * In order to write data to Kafka, it needs to be serialized into a byte array. The way to do this is to implement a
 * Serializer that converts the application object ( {@link MonitoringEvent} in this case) to a byte array. <br />
 * Note that for Headwater clean topics, the message contains the Avro-serialized data, which can easily be created
 * using the {@link AvroSerDe}
 */
public class AvroSerializer implements Serializer<MonitoringEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroSerializer.class);
  private AvroSerDe serializer = null;

  /**
   * @see org.apache.kafka.common.serialization.Serializer#configure(java.util.Map, boolean)
   */
  @Override
  public void configure(Map<String, ?> topic, boolean isKey) {
    if (isKey) {
      return;
    }
    try {
      // Creating the AvroSerDe, setting it with our schema (the MonitoringEvent schema in our case)
      serializer = new AvroSerDe(MonitoringEvent.getClassSchema());
    } catch (Exception e) {
      LOG.warn("Failed to create avro serde for ", e);
      serializer = null;
    }

  }

  /**
   * @see org.apache.kafka.common.serialization.Serializer#serialize(java.lang.String, java.lang.Object)
   */
  @Override
  public byte[] serialize(String topic, MonitoringEvent data) {
    try {
      return serializer.serialize(data);
    } catch (IOException e) {
      LOG.warn("Error while serializing message: " + data, e);
      return null;
    }
  }

  /**
   * @see org.apache.kafka.common.serialization.Serializer#close()
   */
  @Override
  public void close() {
    // Close Method
  }
}
