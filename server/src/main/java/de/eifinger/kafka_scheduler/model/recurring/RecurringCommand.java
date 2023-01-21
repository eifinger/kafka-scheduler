package de.eifinger.kafka_scheduler.model.recurring;

import de.eifinger.kafka_scheduler.model.command.ScheduleCommand;
import org.apache.kafka.common.header.Headers;
import org.jobrunr.jobs.lambdas.JobRequest;

import java.util.Arrays;
import java.util.Objects;

public record RecurringCommand(String id, String topic, byte[] key, byte[] value, Headers headers,
                               String cron) implements ScheduleCommand, JobRequest {

    public static final String CRON = "cron";
    @Override
    public String toString() {
        return "RecurringCommand{" +
                "id='" + id + '\'' +
                ", targetTopic='" + topic + '\'' +
                ", key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                ", headers=" + headers +
                ", cron='" + cron + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecurringCommand that = (RecurringCommand) o;
        return id.equals(that.id) && topic.equals(that.topic) && Arrays.equals(key, that.key) && Arrays.equals(value, that.value) && headers.equals(that.headers) && cron.equals(that.cron);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, topic, headers, cron);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public Class<RecurringCommandHandler> getJobRequestHandler() {
        return RecurringCommandHandler.class;
    }
}
