package de.eifinger.kafka_scheduler.model.fixed_rate;

import de.eifinger.kafka_scheduler.model.command.ScheduleCommand;
import org.apache.kafka.common.header.Headers;
import org.jobrunr.jobs.lambdas.JobRequest;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

public record FixedRateCommand(String id, String topic, byte[] key, byte[] value, Headers headers,
                               Duration period) implements ScheduleCommand, JobRequest {

    public static final String PERIOD = "kafka-scheduler-perdiod";

    @Override
    public Class<FixedRateCommandHandler> getJobRequestHandler() {
        return FixedRateCommandHandler.class;
    }

    @Override
    public String toString() {
        return "FixedRateCommand{" +
                "id='" + id + '\'' +
                ", topic='" + topic + '\'' +
                ", key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                ", headers=" + headers +
                ", period=" + period +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FixedRateCommand that = (FixedRateCommand) o;
        return id.equals(that.id) && topic.equals(that.topic) && Arrays.equals(key, that.key) && Arrays.equals(value, that.value) && headers.equals(that.headers) && period.equals(that.period);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, topic, headers, period);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
