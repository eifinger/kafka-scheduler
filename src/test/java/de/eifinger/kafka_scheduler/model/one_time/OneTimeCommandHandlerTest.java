package de.eifinger.kafka_scheduler.model.one_time;

import de.eifinger.kafka_scheduler.kafka.producer.CommandDeletionProducer;
import de.eifinger.kafka_scheduler.kafka.producer.ReplyProducer;
import de.eifinger.kafka_scheduler.model.command.CommandCompletionHandler;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OneTimeCommandHandlerTest {
    @Mock
    private CommandCompletionHandler commandCompletionHandler;
    @Mock
    private ReplyProducer replyProducer;
    @Mock
    private CommandDeletionProducer commandDeletionProducer;

    @Test
    void testReplyIsSend(){
        var topic = "Topic";
        var targetKey = "uuid".getBytes(StandardCharsets.UTF_8);
        var value = "value".getBytes(StandardCharsets.UTF_8);
        var header = new RecordHeader("customHeader", "customHeader".getBytes(StandardCharsets.UTF_8));
        var headers = new RecordHeaders(List.of(header));
        var scheduleCommand = new OneTimeCommand("0815", topic, targetKey, value, headers, LocalDateTime.now());

        var expectedProducerRecord = new ProducerRecord<>(topic, targetKey,
                value);
        expectedProducerRecord.headers().add(header);
        var sut = new OneTimeCommandHandler(replyProducer, commandCompletionHandler, commandDeletionProducer, "one-time-topic");
        sut.run(scheduleCommand);

        verify(replyProducer).send(expectedProducerRecord);
    }

    @Test
    void testTaskCompletedCallbackIsCalled(){
        var key = "0815";
        var targetKey = "uuid".getBytes(StandardCharsets.UTF_8);
        var value = "value".getBytes(StandardCharsets.UTF_8);
        var header = new RecordHeader("customHeader", "customHeader".getBytes(StandardCharsets.UTF_8));
        var headers = new RecordHeaders(List.of(header));
        var scheduleCommand = new OneTimeCommand(key, "Topic", targetKey, value, headers, LocalDateTime.now());
        var sut = new OneTimeCommandHandler(replyProducer, commandCompletionHandler, commandDeletionProducer, "one-time-topic");
        sut.run(scheduleCommand);

        verify(commandCompletionHandler).commandCompleted(scheduleCommand);
    }

    @Test
    void testScheduleCommandIsDeleted(){
        var oneTimeTopic = "one-time-topic";
        var key = "0815";
        var topic = "Topic";
        var targetKey = "uuid".getBytes(StandardCharsets.UTF_8);
        var value = "value".getBytes(StandardCharsets.UTF_8);
        var header = new RecordHeader("customHeader", "customHeader".getBytes(StandardCharsets.UTF_8));
        var headers = new RecordHeaders(List.of(header));
        var scheduleCommand = new OneTimeCommand(key, topic, targetKey, value, headers, LocalDateTime.now());
        ProducerRecord<String, Void> expectedProducerRecord = new ProducerRecord<>(oneTimeTopic, key,
                null);

        var sut = new OneTimeCommandHandler(replyProducer, commandCompletionHandler, commandDeletionProducer, oneTimeTopic);
        sut.run(scheduleCommand);

        verify(commandDeletionProducer).send(expectedProducerRecord);
    }

    @Test
    void testScheduleCommandIsNotDeletedWhenReplyIsNotSent(){
        var scheduleCommand = new OneTimeCommand("0815", "Topic", new byte[]{}, new byte[]{}, new RecordHeaders(), LocalDateTime.now());

        doThrow(KafkaException.class).when(replyProducer).send(any());

        var sut = new OneTimeCommandHandler(replyProducer, commandCompletionHandler, commandDeletionProducer, "one-time-topic");
        sut.run(scheduleCommand);

        verify(commandDeletionProducer, never()).send(any());
    }
}
