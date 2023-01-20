package de.eifinger.kafka_scheduler.kafka.consumer;

import de.eifinger.kafka_scheduler.model.command.ScheduleCommand;
import de.eifinger.kafka_scheduler.model.command.SchedulerId;
import nl.altindag.console.ConsoleCaptor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked"})
@ExtendWith(MockitoExtension.class)
class GenericScheduleCommandConsumerTest {

    @Mock
    private Consumer<String, ScheduleCommand> consumer;

    @Test
    void testCaughtUpConsumerDoesSchedule() {
        var sut = spy(new CommandConsumerMock());
        var key = "0815";
        var consumerRecord = new ConsumerRecord<>("command-topic", 0, 0, key, new byte[]{});

        // Define that there are no messages in the assigned partition
        when(consumer.endOffsets(any())).thenReturn(Map.of(new TopicPartition("Topic", 0), 0L));
        // Assign a single partition
        sut.onPartitionsAssigned(consumer, Collections.singletonList(new TopicPartition("Topic", 0)));

        sut.receive(consumerRecord);

        verify(sut).schedule(any(ConsumerRecord.class));
    }

    @Test
    void testNonCaughtUpConsumerDoesNotScheduleDirectly() {
        var sut = spy(new CommandConsumerMock());
        var key = "0815";
        var consumerRecord = new ConsumerRecord<>("command-topic", 0, 0, key, new byte[]{});

        // Define that there are two messages in the assigned partition
        when(consumer.endOffsets(any())).thenReturn(Map.of(new TopicPartition("Topic", 0), 2L));
        // Assign a single partition
        sut.onPartitionsAssigned(consumer, Collections.singletonList(new TopicPartition("Topic", 0)));

        sut.receive(consumerRecord);

        verify(sut, never()).schedule(any(ConsumerRecord.class));
    }

    @Test
    void testNonCaughtUpConsumerDoesScheduleWhenCaughtUp() {
        var sut = spy(new CommandConsumerMock());
        var firstRecord = new ConsumerRecord<>("command-topic", 0, 0, "0815", new byte[]{});
        var secondRecord = new ConsumerRecord<>("command-topic", 0, 1, "0816", new byte[]{});

        // Define that there are two messages in the assigned partition
        when(consumer.endOffsets(any())).thenReturn(Map.of(new TopicPartition("Topic", 0), 2L));
        // Assign a single partition
        sut.onPartitionsAssigned(consumer, Collections.singletonList(new TopicPartition("Topic", 0)));

        // Receive the two messages
        sut.receive(firstRecord);
        sut.receive(secondRecord);

        verify(sut, times(2)).schedule(any(ConsumerRecord.class));
    }

    @Test
    void testNonCaughtUpConsumerDoesNotScheduleWhenCancelled() {
        var sut = spy(new CommandConsumerMock());
        var key = "0815";
        var scheduleCommandConsumerRecord = new ConsumerRecord<>("command-topic", 0, 0, key, new byte[]{});
        ConsumerRecord<String, byte[]> cancelCommandConsumerRecord = new ConsumerRecord<>("command-topic", 0, 1, key, null);

        // Define that there are two messages in the assigned partition
        when(consumer.endOffsets(any())).thenReturn(Map.of(new TopicPartition("Topic", 0), 2L));
        // Assign a single partition
        sut.onPartitionsAssigned(consumer, Collections.singletonList(new TopicPartition("Topic", 0)));

        sut.receive(scheduleCommandConsumerRecord);
        sut.receive(cancelCommandConsumerRecord);

        verify(sut, never()).schedule(any(ConsumerRecord.class));
    }

    @Test
    void testCancel() {
        var sut = spy(new CommandConsumerMock());
        var key = "0815";
        var schedulerId = new SchedulerId("uuid");
        var scheduleCommandConsumerRecord = new ConsumerRecord<>("command-topic", 0, 0, key, new byte[]{});
        ConsumerRecord<String, byte[]> cancelCommandConsumerRecord = new ConsumerRecord<>("command-topic", 0, 1, key, null);

        when(sut.schedule(any(ConsumerRecord.class))).thenReturn(schedulerId);
        // Define that there are no messages in the assigned partition
        when(consumer.endOffsets(any())).thenReturn(Map.of(new TopicPartition("Topic", 0), 0L));
        // Assign a single partition
        sut.onPartitionsAssigned(consumer, Collections.singletonList(new TopicPartition("Topic", 0)));

        sut.receive(scheduleCommandConsumerRecord);
        sut.receive(cancelCommandConsumerRecord);

        verify(sut).schedule(any(ConsumerRecord.class));
        verify(sut).cancel(schedulerId);
    }

    @Test
    void testPartitionsRevokeCancelsCommands() {
        var sut = spy(new CommandConsumerMock());
        var key = "0815";
        var schedulerId = new SchedulerId("uuid");
        var scheduleCommandConsumerRecord = new ConsumerRecord<>("command-topic", 0, 0, key, new byte[]{});
        var topicPartitionList = Collections.singletonList(new TopicPartition("Topic", 0));

        when(sut.schedule(any(ConsumerRecord.class))).thenReturn(schedulerId);
        // Define that there are no messages in the assigned partition
        when(consumer.endOffsets(any())).thenReturn(Map.of(new TopicPartition("Topic", 0), 0L));
        // Assign a single partition
        sut.onPartitionsAssigned(consumer, topicPartitionList);

        sut.receive(scheduleCommandConsumerRecord);
        sut.onPartitionsRevoked(consumer, topicPartitionList);

        verify(sut).schedule(any(ConsumerRecord.class));
        verify(sut).cancel(schedulerId);
    }

    @Test
    void testErrorGetsLoggedOnException() {
        try (var consoleCaptor = new ConsoleCaptor()) {
            var sut = spy(new CommandConsumerMock());
            var key = "0815";
            var consumerRecord = new ConsumerRecord<>("command-topic", 0, 0, key, new byte[]{});

            // Define that there are no messages in the assigned partition
            when(consumer.endOffsets(any())).thenReturn(Map.of(new TopicPartition("Topic", 0), 0L));
            // Deriving class throws exception
            when(sut.schedule(any(ConsumerRecord.class))).thenThrow(RuntimeException.class);
            // Assign a single partition
            sut.onPartitionsAssigned(consumer, Collections.singletonList(new TopicPartition("Topic", 0)));

            sut.receive(consumerRecord);
            assertThat(consoleCaptor.getStandardOutput())
                    .anyMatch( el -> el.contains("Could not schedule the command"));
        }
    }

}
