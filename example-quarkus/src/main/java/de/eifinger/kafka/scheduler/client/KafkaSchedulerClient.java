package de.eifinger.kafka.scheduler.client;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.enterprise.context.Dependent;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.UUID;


@Dependent
public class KafkaSchedulerClient<K, V> {

    KafkaProducer<String, V> kafkaProducer;
    static final String KEY = "kafka-scheduler-key";
    static final String TOPIC = "kafka-scheduler-topic";
    static final String WHEN = "kafka-scheduler-when";

    final String oneTimeTopic;

    public KafkaSchedulerClient(KafkaProducer<String, V> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
        this.oneTimeTopic = "command.scheduler.one-time-schedule-commands.0";
    }

    public SchedulerId schedule(K key, V value, String topic, LocalDateTime when) {
        var id = new SchedulerId(UUID.randomUUID().toString());
        var producerRecord = new ProducerRecord<>(oneTimeTopic, id.toString(), value);
        producerRecord.headers().add(TOPIC, topic.getBytes(StandardCharsets.UTF_8));
        producerRecord.headers().add(WHEN, when.toString().getBytes(StandardCharsets.UTF_8));
        producerRecord.headers().add(KEY, key.toString().getBytes(StandardCharsets.UTF_8));
        kafkaProducer.send(producerRecord);
        return id;
    }
}
