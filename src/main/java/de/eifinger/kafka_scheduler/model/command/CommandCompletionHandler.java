package de.eifinger.kafka_scheduler.model.command;

public interface CommandCompletionHandler {
    void commandCompleted(ScheduleCommand command);
}
