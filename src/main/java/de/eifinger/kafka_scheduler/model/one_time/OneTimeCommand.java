package de.eifinger.kafka_scheduler.model.one_time;

import de.eifinger.kafka_scheduler.model.command.ScheduleCommand;
import org.apache.kafka.common.header.Headers;
import org.jobrunr.jobs.lambdas.JobRequest;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;

public record OneTimeCommand(String id, String topic, byte[] key, byte[] value, Headers headers,
                             LocalDateTime when) implements ScheduleCommand, JobRequest {

    public static final String WHEN = "kafka-scheduler-when";

    @Override
    public Class<OneTimeCommandHandler> getJobRequestHandler() {
        return OneTimeCommandHandler.class;
    }

    @Override
    public String toString() {
        return "OneTimeCommand{" +
                "id='" + id + '\'' +
                ", targetTopic='" + topic + '\'' +
                ", key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                ", headers=" + headers +
                ", when=" + when +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OneTimeCommand that = (OneTimeCommand) o;
        return id.equals(that.id) && topic.equals(that.topic) && Arrays.equals(key, that.key) && Arrays.equals(value, that.value) && headers.equals(that.headers) && when.equals(that.when);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, topic, headers, when);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
