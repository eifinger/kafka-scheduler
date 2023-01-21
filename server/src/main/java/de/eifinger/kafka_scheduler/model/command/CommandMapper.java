package de.eifinger.kafka_scheduler.model.command;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface CommandMapper<T extends ScheduleCommand> {
    T toScheduleCommand(ConsumerRecord<String, byte[]> consumerRecord);
}
