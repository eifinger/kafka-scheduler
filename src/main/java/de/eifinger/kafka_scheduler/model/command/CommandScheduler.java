package de.eifinger.kafka_scheduler.model.command;

public interface CommandScheduler<T extends ScheduleCommand> {
    SchedulerId schedule(T scheduleCommand);

    void cancel(SchedulerId schedulerId);
}
