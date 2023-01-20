package de.eifinger.kafka_scheduler.model.command;

import org.apache.kafka.common.header.Headers;

public interface ScheduleCommand {

    String id();

    String topic();

    byte[] key();

    byte[] value();

    Headers headers();
}
