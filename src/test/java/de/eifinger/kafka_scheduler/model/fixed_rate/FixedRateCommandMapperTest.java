package de.eifinger.kafka_scheduler.model.fixed_rate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.KEY;
import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.TOPIC;
import static de.eifinger.kafka_scheduler.model.fixed_rate.FixedRateCommand.PERIOD;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class FixedRateCommandMapperTest {

    @Test
    void testMapping() {
        var sut = new FixedRateCommandMapper();

        var period = Duration.ofSeconds(5);
        var periodString = period.toString();
        var key = "0815";
        var topic = "reply-topic";
        var payload = new byte[]{};
        var targetKey = new byte[]{};
        var record = new ConsumerRecord<>("command-topic", 0, 0, key, payload);
        record.headers().add(TOPIC, topic.getBytes(StandardCharsets.UTF_8));
        record.headers().add(PERIOD, periodString.getBytes(StandardCharsets.UTF_8));
        record.headers().add(KEY, targetKey);
        var customHeader = new RecordHeader("custom", new byte[]{});
        record.headers().add(customHeader);

        var command = sut.toScheduleCommand(record);
        assertThat(command.id()).isEqualTo(key);
        assertThat(command.topic()).isEqualTo(topic);
        assertThat(command.value()).isEqualTo(payload);
        assertThat(command.period()).isEqualTo(period);
        assertThat(command.key()).isEqualTo(targetKey);
        assertThat(command.headers()).containsOnly(customHeader);
    }
}
