package de.eifinger.kafka_scheduler_client;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;

import static de.eifinger.kafka_scheduler_client.KafkaSchedulerClient.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KafkaSchedulerClientTest {

    @Mock
    KafkaProducer<String> kafkaProducer;

    @Mock
    KafkaTombstoneProducer tombstoneProducer;

    @InjectMocks
    KafkaSchedulerClient<String> sut;

    @Captor
    ArgumentCaptor<ProducerRecord<String, String>> captor;

    @Captor
    ArgumentCaptor<ProducerRecord<String, Object>> tombStoneCaptor;

    @Test
    void scheduleOneTimeCreatesCorrectProducerRecord() {
        var replyTopic = "replyTopic";
        var when = LocalDateTime.now().plusSeconds(5);
        var value = "value";
        var key = "myKey".getBytes(StandardCharsets.UTF_8);

        var schedulerId = sut.schedule(key, value, replyTopic, when);

        assertThat(schedulerId.id()).isNotBlank();
        assertThat(schedulerId.commandType()).isEqualTo(CommandType.ONE_TIME);

        verify(kafkaProducer, times(1)).send(captor.capture());

        var producerRecord = captor.getValue();
        assertThat(producerRecord.key()).isNotBlank();
        assertThat(producerRecord.value()).isEqualTo(value);
        assertThat(producerRecord.headers().lastHeader(TOPIC).value()).isEqualTo(replyTopic.getBytes(StandardCharsets.UTF_8));
        assertThat(producerRecord.headers().lastHeader(WHEN).value()).isEqualTo(when.toString().getBytes(StandardCharsets.UTF_8));
        assertThat(producerRecord.headers().lastHeader(KEY).value()).isEqualTo(key);
    }

    @Test
    void scheduleRecurringCreatesCorrectProducerRecord() {
        var replyTopic = "replyTopic";
        var cron = "*/5 * * * * *";
        var value = "value";
        var key = "myKey".getBytes(StandardCharsets.UTF_8);

        var schedulerId = sut.schedule(key, value, replyTopic, cron);

        assertThat(schedulerId.id()).isNotBlank();
        assertThat(schedulerId.commandType()).isEqualTo(CommandType.RECURRING);

        verify(kafkaProducer, times(1)).send(captor.capture());

        var producerRecord = captor.getValue();
        assertThat(producerRecord.key()).isNotBlank();
        assertThat(producerRecord.value()).isEqualTo(value);
        assertThat(producerRecord.headers().lastHeader(TOPIC).value()).isEqualTo(replyTopic.getBytes(StandardCharsets.UTF_8));
        assertThat(producerRecord.headers().lastHeader(CRON).value()).isEqualTo(cron.getBytes(StandardCharsets.UTF_8));
        assertThat(producerRecord.headers().lastHeader(KEY).value()).isEqualTo(key);
    }

    @Test
    void scheduleFixedRateCreatesCorrectProducerRecord() {
        var replyTopic = "replyTopic";
        var period = Duration.ofSeconds(5);
        var value = "value";
        var key = "myKey".getBytes(StandardCharsets.UTF_8);

        var schedulerId = sut.schedule(key, value, replyTopic, period);

        assertThat(schedulerId.id()).isNotBlank();
        assertThat(schedulerId.commandType()).isEqualTo(CommandType.FIXED_RATE);

        verify(kafkaProducer, times(1)).send(captor.capture());

        var producerRecord = captor.getValue();
        assertThat(producerRecord.key()).isNotBlank();
        assertThat(producerRecord.value()).isEqualTo(value);
        assertThat(producerRecord.headers().lastHeader(TOPIC).value()).isEqualTo(replyTopic.getBytes(StandardCharsets.UTF_8));
        assertThat(producerRecord.headers().lastHeader(PERIOD).value()).isEqualTo(period.toString().getBytes(StandardCharsets.UTF_8));
        assertThat(producerRecord.headers().lastHeader(KEY).value()).isEqualTo(key);
    }

    @ParameterizedTest
    @EnumSource(CommandType.class)
    void cancelCreatesCorrectProducerRecord(CommandType commandType) {
        var id = "id";
        var schedulerId = new SchedulerId(id, commandType);

        sut.cancel(schedulerId);

        verify(tombstoneProducer, times(1)).send(tombStoneCaptor.capture());

        var producerRecord = tombStoneCaptor.getValue();
        assertThat(producerRecord.key()).isEqualTo(id);
        assertThat(producerRecord.value()).isNull();
        assertThat(producerRecord.topic()).satisfies(topic -> {
            switch (commandType) {
                case ONE_TIME -> assertThat(topic).isEqualTo("command.scheduler.one-time-schedule-commands.0");
                case RECURRING -> assertThat(topic).isEqualTo("command.scheduler.recurring-schedule-commands.0");
                case FIXED_RATE -> assertThat(topic).isEqualTo("command.scheduler.fixed-rate-schedule-commands.0");
            }
        });
    }
}
