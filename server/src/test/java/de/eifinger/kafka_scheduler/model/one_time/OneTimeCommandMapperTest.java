package de.eifinger.kafka_scheduler.model.one_time;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.KEY;
import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.TOPIC;
import static de.eifinger.kafka_scheduler.model.one_time.OneTimeCommand.WHEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OneTimeCommandMapperTest {

    private final static LocalDate LOCAL_DATE = LocalDate.of(1990, 7, 20);
    private static final Clock fixedClock = Clock.fixed(LOCAL_DATE.atStartOfDay(ZoneId.systemDefault()).toInstant(), ZoneId.systemDefault());
    @Mock
    private Clock clock;

    @Test
    void testMapping() {
        var sut = new OneTimeCommandMapper();
        var key = "0815";
        var topic = "reply-topic";
        var payload = new byte[]{};
        var targetKey = new byte[]{};
        when(clock.instant()).thenReturn(fixedClock.instant());
        when(clock.getZone()).thenReturn(fixedClock.getZone());
        var when = LocalDateTime.now(clock);
        var record = new ConsumerRecord<>("command-topic", 0, 0, key, payload);
        record.headers().add(TOPIC, topic.getBytes(StandardCharsets.UTF_8));
        record.headers().add(WHEN, when.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(KEY, targetKey);
        var customHeader = new RecordHeader("custom", new byte[]{});
        record.headers().add(customHeader);

        var command = sut.toScheduleCommand(record);
        assertThat(command.id()).isEqualTo(key);
        assertThat(command.topic()).isEqualTo(topic);
        assertThat(command.value()).isEqualTo(payload);
        assertThat(command.when()).isEqualTo(when);
        assertThat(command.key()).isEqualTo(targetKey);
        assertThat(command.headers()).containsOnly(customHeader);
    }
}
