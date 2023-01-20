package de.eifinger.kafka_scheduler;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.KEY;
import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.TOPIC;
import static de.eifinger.kafka_scheduler.model.fixed_rate.FixedRateCommand.PERIOD;
import static de.eifinger.kafka_scheduler.model.one_time.OneTimeCommand.WHEN;
import static de.eifinger.kafka_scheduler.model.recurring.RecurringCommand.CRON;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@QuarkusTestResource(KafkaCompanionResource.class)
class IntegrationTest {

    @InjectKafkaCompanion
    KafkaCompanion companion;

    @Test
    void testRecurringCommand() {
        var cron = "*/5 * * * * *"; // every 5s
        var id = "testRecurringCommand";
        var replyTopic = "recurring-reply-topic";
        var value = "test".getBytes(StandardCharsets.UTF_8);
        var record = new ProducerRecord<>("command.scheduler.recurring-schedule-commands.0", id, value);
        record.headers().add(TOPIC, replyTopic.getBytes(StandardCharsets.UTF_8));
        record.headers().add(CRON, cron.getBytes(StandardCharsets.UTF_8));
        record.headers().add(KEY, value);
        companion.produce(String.class, byte[].class).fromRecords(List.of(record));

        var reply = companion.consumeStrings().fromTopics(replyTopic, Duration.ofSeconds(32));
        reply.awaitNextRecords(2, Duration.ofSeconds(32));
        assertEquals(2, reply.count());
    }

    @Test
    void testOneTimeCommand() {
        var replyTopic = "test-one-time-command";
        companion.produce(String.class, byte[].class).fromRecords(List.of(oneTimeRecord("1", replyTopic)));

        var reply = companion.consumeStrings().fromTopics(replyTopic, Duration.ofSeconds(16));
        reply.awaitNextRecord(Duration.ofSeconds(16));
        assertEquals(1, reply.count());
    }

    @Test
    void testFixedRateCommand() {
        var period = Duration.ofSeconds(5);
        var periodString = period.toString();
        var id = "testFixedRateCommand";
        var replyTopic = "fixed-rate-reply-topic";
        var value = new byte[]{};
        var record = new ProducerRecord<>("command.scheduler.fixed-rate-schedule-commands.0", id, value);
        record.headers().add(TOPIC, replyTopic.getBytes(StandardCharsets.UTF_8));
        record.headers().add(PERIOD, periodString.getBytes(StandardCharsets.UTF_8));
        record.headers().add(KEY, value);
        companion.produce(String.class, byte[].class).fromRecords(List.of(record));

        var reply = companion.consumeStrings().fromTopics(replyTopic, Duration.ofSeconds(32));
        reply.awaitNextRecords(2, Duration.ofSeconds(32));
        assertEquals(2, reply.count());
    }

    @Test
    void testCancel() {
        var replyTopic = "test-cancel";
        companion.produce(String.class, byte[].class)
                .fromRecords(
                List.of(
                        oneTimeRecord("1", replyTopic),
                        oneTimeRecord("2", replyTopic))
        ).awaitCompletion();
        companion.produceWithSerializers(StringSerializer.class, VoidSerializer.class)
                .fromRecords(List.of(deleteRecord()))
                .awaitCompletion();

        var reply = companion.consumeStrings().fromTopics(replyTopic, Duration.ofSeconds(10));
        reply.awaitCompletion(Duration.ofSeconds(10));
        assertEquals(1, reply.count());
    }

    private ProducerRecord<String, byte[]> oneTimeRecord(String id, String replyTopic) {
        var when = LocalDateTime.now().plusSeconds(5);
        var value = id.getBytes(StandardCharsets.UTF_8);
        var record = new ProducerRecord<>("command.scheduler.one-time-schedule-commands.0", id, value);
        record.headers().add(TOPIC, replyTopic.getBytes(StandardCharsets.UTF_8));
        record.headers().add(WHEN, when.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(KEY, value);
        return record;
    }

    private ProducerRecord<Object, Object> deleteRecord() {
        return new ProducerRecord<>("command.scheduler.one-time-schedule-commands.0", "1", null);
    }
}
