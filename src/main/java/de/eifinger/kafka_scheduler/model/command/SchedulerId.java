package de.eifinger.kafka_scheduler.model.command;

import org.jobrunr.jobs.JobId;

import java.util.UUID;

public record SchedulerId(String id) {

    public JobId toJobId() {
        return new JobId(UUID.fromString(id()));
    }

    public SchedulerId(JobId id) {
        this(id.toString());
    }
}
