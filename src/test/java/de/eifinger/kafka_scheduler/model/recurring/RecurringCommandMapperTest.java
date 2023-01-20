package de.eifinger.kafka_scheduler.model.recurring;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;

import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.KEY;
import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.TOPIC;
import static de.eifinger.kafka_scheduler.model.recurring.RecurringCommand.CRON;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class RecurringCommandMapperTest {

    @Test
    void testMapping() {
        var sut = new RecurringCommandMapper();

        var cron = "5 4 * * *";
        var key = "0815";
        var topic = "reply-topic";
        var payload = new byte[]{};
        var targetKey = new byte[]{};
        var record = new ConsumerRecord<>("command-topic", 0, 0, key, payload);
        record.headers().add(TOPIC, topic.getBytes(StandardCharsets.UTF_8));
        record.headers().add(CRON, cron.getBytes(StandardCharsets.UTF_8));
        record.headers().add(KEY, targetKey);
        var customHeader = new RecordHeader("custom", new byte[]{});
        record.headers().add(customHeader);

        var command = sut.toScheduleCommand(record);
        assertThat(command.id()).isEqualTo(key);
        assertThat(command.topic()).isEqualTo(topic);
        assertThat(command.value()).isEqualTo(payload);
        assertThat(command.cron()).isEqualTo(cron);
        assertThat(command.key()).isEqualTo(targetKey);
        assertThat(command.headers()).containsOnly(customHeader);
    }
}
