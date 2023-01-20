package de.eifinger.kafka_scheduler.kafka.consumer;

import de.eifinger.kafka_scheduler.model.command.CommandCompletionHandler;
import de.eifinger.kafka_scheduler.model.command.ScheduleCommand;
import de.eifinger.kafka_scheduler.model.command.SchedulerId;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Consumes schedule command messages from a Kafka topic, deriving classes call a scheduler to schedule them and cancel
 * the commands if tombstone messages arrive or the partition gets revoked during a re-balancing process.
 * <p>
 * The consumer will always start at offset zero to make sure that all tasks in the assigned partition are handled.
 * <p>
 * This class only works with compacted topics.
 */
public abstract class GenericScheduleCommandConsumer implements KafkaConsumerRebalanceListener, CommandCompletionHandler {

    protected final HashMap<Integer, HashMap<String, SchedulerId>> scheduledCommandsByPartitionByCommandId = new HashMap<>();
    private final HashMap<Integer, HashMap<String, ConsumerRecord<String, byte[]>>> pendingConsumerRecordsByPartitionByKey = new HashMap<>();
    private final Logger log = org.slf4j.LoggerFactory.getLogger(getClass());
    private Map<Integer, Long> latestOffsetsByPartition;

    abstract SchedulerId schedule(ConsumerRecord<String, byte[]> consumerRecord);

    abstract void cancel(SchedulerId id);

    /**
     * Should be overridden and annotated with @Incoming in the extending class.
     * Overriding implementation should call super.receive(ConsumerRecord<String, byte[]> record);
     *
     * @param consumerRecord the ScheduleCommand identified by a unique KafkaKey
     */
    public void receive(ConsumerRecord<String, byte[]> consumerRecord) {
        log.debug("Received message. Topic: {}, partition: {}, record offset: {}", consumerRecord.topic(), consumerRecord.partition(),
                consumerRecord.offset());
        // Is this a tombstone record? (Deletes the key from the topic)
        if (consumerRecord.value() == null) {
            cancelScheduledCommand(consumerRecord);
        } else {
            handleRecord(consumerRecord);
        }
    }

    /**
     * If this partition is caught up schedule the command.
     * If not store the command in the hashmap.
     * As soon as the partition is caught up schedule all pending commands.
     *
     * @param consumerRecord ScheduleCommand identified by a unique KafkaKey
     */
    private void handleRecord(ConsumerRecord<String, byte[]> consumerRecord) {
        log.debug("Handling ScheduleCommand.");
        // Have we already seen all the messages for this partition that where there when we started?
        if (isPartitionCaughtUp(consumerRecord)) {
            // Schedule all the pending tasks once and the current task directly
            scheduleAllTasksForPartition(consumerRecord.partition());
            scheduleRecord(consumerRecord);
        } else {
            // If not store it to be scheduled later
            log.info("Stored pending ScheduleCommand {}", consumerRecord.key());
            this.pendingConsumerRecordsByPartitionByKey.get(consumerRecord.partition()).put(consumerRecord.key(), consumerRecord);
        }
    }

    private void scheduleAllTasksForPartition(Integer partitionId) {
        var map = pendingConsumerRecordsByPartitionByKey.get(partitionId);
        if (!map.isEmpty()) {
            log.info("Scheduling all pending commands for partition {}", partitionId);
            for (var entrySet : map.entrySet()) {
                scheduleRecord(entrySet.getValue());
                map.remove(entrySet.getKey());
            }
        }
    }

    private void scheduleRecord(ConsumerRecord<String, byte[]> consumerRecord) {
        try {
            log.info("Scheduling command: {}", consumerRecord.key());
            var id = schedule(consumerRecord);
            this.scheduledCommandsByPartitionByCommandId.get(consumerRecord.partition()).put(consumerRecord.key(), id);
        } catch (Exception exception) {
            log.error("Could not schedule the command", exception);
        }
    }

    /**
     * If partition for this consumerRecord is caught up we have already scheduled the command. So we have to retrieve it from
     * the hashmap and cancel it.
     * If the partition is not caught up just remove the pending schedule command from the map, so it won't get
     * scheduled.
     *
     * @param consumerRecord the ScheduleCommand identified by a unique KafkaKey
     */
    private void cancelScheduledCommand(ConsumerRecord<String, ?> consumerRecord) {
        log.debug("Handling cancellation");
        if (pendingConsumerRecordsByPartitionByKey.get(consumerRecord.partition()).containsKey(consumerRecord.key())) {
            log.info("Unscheduled command was already cancelled: {}", consumerRecord.key());
            pendingConsumerRecordsByPartitionByKey.get(consumerRecord.partition()).remove(consumerRecord.key());
        } else {
            log.info("Scheduled command should be cancelled: {}", consumerRecord.key());
            if (scheduledCommandsByPartitionByCommandId.get(consumerRecord.partition()).containsKey(consumerRecord.key())) {
                var tasksForPartition = scheduledCommandsByPartitionByCommandId.get(consumerRecord.partition());
                var id = tasksForPartition.get(consumerRecord.key());
                cancel(id);
                tasksForPartition.remove(consumerRecord.key());
                log.info("Successfully cancelled command: {}", consumerRecord.key());
            } else {
                log.info("There is no command with the id: {} which could be cancelled. Maybe the topic was not " +
                        "compacted fast enough", consumerRecord.key());
            }
        }
        if (isPartitionCaughtUp(consumerRecord)) {
            scheduleAllTasksForPartition(consumerRecord.partition());
        }
    }

    /**
     * Cancel all scheduled commands for each of the revoked partitions.
     *
     * @param partitions The list of revoked partitions
     */
    @Override
    public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.info("Partitions got revoked");
        for (TopicPartition partition : partitions) {
            log.info("Cancelling commands for partition: {}", partition.partition());
            cancelScheduledCommandsForPartition(partition.partition());
        }
    }

    /**
     * For each assigned partition store the latest offset and set the consumer offset to zero.
     *
     * @param partitions The list of assigned partitions
     */
    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        storeLatestOffsets(consumer, partitions);
        for (TopicPartition partition : partitions) {
            this.scheduledCommandsByPartitionByCommandId.put(partition.partition(), new HashMap<>());
            this.pendingConsumerRecordsByPartitionByKey.put(partition.partition(), new HashMap<>());
            log.info("Setting partition {} to offset zero.", partition.partition());
            consumer.seek(partition, 0);
        }
    }

    /**
     * Store the endOffset. "How many messages are in the partition at this moment?"
     * Given this information we can compute when this consumer is caught up, has seen
     * all messages and can start scheduling tasks.
     * Without knowing that information we might see schedule commands which will be cancelled a few messages later,
     * so we should not schedule them at all.
     * This can happen when the topic is not compacted fast enough.
     *
     * @param partitions A list of assigned partitions
     */
    private void storeLatestOffsets(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        this.latestOffsetsByPartition =
                consumer.endOffsets(partitions)
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                e -> e.getKey().partition(), // Get the partition id
                                Map.Entry::getValue // Get the endOffset
                        ));
    }

    private void cancelScheduledCommandsForPartition(Integer partition) {
        for (var id : this.scheduledCommandsByPartitionByCommandId.get(partition).keySet()) {
            var scheduledId = scheduledCommandsByPartitionByCommandId.get(partition).get(id);
            cancel(scheduledId);
            this.scheduledCommandsByPartitionByCommandId
                    .get(partition)
                    .remove(id);
            log.info("Cancelled command with id: {}.", id);
        }
    }

    /**
     * Check if the offset of the received consumerRecord is at least the same as the latest offset of the partition of this
     * consumerRecord at the time of this consumers start up.
     * The offset has to be increased by one because if the partition contains one consumerRecord its offset will be at 1
     * meaning "after the `0` offset". But the consumerRecord itself is at offset 0.
     *
     * @param consumerRecord The received Record
     * @return True if the offset of the received consumerRecord is at least the same as the latest offset of the partition of this
     * * consumerRecord at the time of this consumers start up.
     */
    private boolean isPartitionCaughtUp(ConsumerRecord<String, ?> consumerRecord) {
        return consumerRecord.offset() + 1 >= this.latestOffsetsByPartition.get(consumerRecord.partition());
    }

    /**
     * Callback to remove the ScheduleCommand from the map of scheduled task to prevent it growing without bounds.
     *
     * @param command The ScheduleCommand which completed
     */
    @Override
    public void commandCompleted(ScheduleCommand command) {
        for (var scheduledTasksByKey : this.scheduledCommandsByPartitionByCommandId.values()) {
            scheduledTasksByKey.remove(command.id());
        }
    }
}

