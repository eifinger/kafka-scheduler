package de.eifinger.kafka_scheduler_client;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public class KafkaSchedulerClient<V> {

    KafkaProducer<V> kafkaProducer;
    KafkaTombstoneProducer tombstoneProducer;
    static final String KEY = "kafka-scheduler-key";
    static final String TOPIC = "kafka-scheduler-topic";
    static final String WHEN = "kafka-scheduler-when";
    static final String CRON = "kafka-scheduler-cron";
    static final String PERIOD = "kafka-scheduler-perdiod";

    final String oneTimeTopic;
    final String recurringTopic;
    final String fixedRateTopic;

    public KafkaSchedulerClient(KafkaProducer<V> kafkaProducer, KafkaTombstoneProducer tombstoneProducer) {
        this.kafkaProducer = kafkaProducer;
        this.tombstoneProducer = tombstoneProducer;
        this.oneTimeTopic = "command.scheduler.one-time-schedule-commands.0";
        this.recurringTopic = "command.scheduler.recurring-schedule-commands.0";
        this.fixedRateTopic = "command.scheduler.fixed-rate-schedule-commands.0";
    }

    public SchedulerId schedule(byte[] key, V value, String topic, LocalDateTime when) {
        var schedulerId = new SchedulerId(UUID.randomUUID().toString(), CommandType.ONE_TIME);
        var periodHeader = new RecordHeader(WHEN, when.toString().getBytes(StandardCharsets.UTF_8));
        return scheduleWithAdditionalHeaders(schedulerId, key, value, topic, List.of(periodHeader));
    }

    public SchedulerId schedule(byte[] key, V value, String topic, String cron) {
        var schedulerId = new SchedulerId(UUID.randomUUID().toString(), CommandType.RECURRING);
        var periodHeader = new RecordHeader(CRON, cron.getBytes(StandardCharsets.UTF_8));
        return scheduleWithAdditionalHeaders(schedulerId, key, value, topic, List.of(periodHeader));
    }

    public SchedulerId schedule(byte[] key, V value, String topic, Duration period) {
        var schedulerId = new SchedulerId(UUID.randomUUID().toString(), CommandType.FIXED_RATE);
        var periodHeader = new RecordHeader(PERIOD, period.toString().getBytes(StandardCharsets.UTF_8));
        return scheduleWithAdditionalHeaders(schedulerId, key, value, topic, List.of(periodHeader));
    }

    public void cancel(SchedulerId schedulerId) {
        var producerRecord = new ProducerRecord<>(topicForCommandType(schedulerId.commandType()), schedulerId.id(), null);
        tombstoneProducer.send(producerRecord);
    }

    private SchedulerId scheduleWithAdditionalHeaders(SchedulerId schedulerId, byte[] key, V value, String topic, Iterable<RecordHeader> additionalHeaders) {
        var producerRecord = new ProducerRecord<>(oneTimeTopic, schedulerId.id(), value);
        producerRecord.headers().add(TOPIC, topic.getBytes(StandardCharsets.UTF_8));
        producerRecord.headers().add(KEY, key);
        additionalHeaders.forEach(header -> producerRecord.headers().add(header));
        kafkaProducer.send(producerRecord);
        return schedulerId;
    }

    private String topicForCommandType(CommandType commandType) {
        return switch (commandType) {
            case ONE_TIME -> oneTimeTopic;
            case RECURRING -> recurringTopic;
            case FIXED_RATE -> fixedRateTopic;
        };
    }
}
