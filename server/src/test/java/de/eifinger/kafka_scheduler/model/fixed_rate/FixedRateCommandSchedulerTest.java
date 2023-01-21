package de.eifinger.kafka_scheduler.model.fixed_rate;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FixedRateCommandSchedulerTest {

    @InjectMocks
    FixedRateCommandScheduler sut;
    @Mock
    JobRequestScheduler scheduler;

    @Test
    void testCommandIsScheduled(){
        var period = Duration.ofSeconds(5);
        var command = new FixedRateCommand("id", "topic", new byte[]{}, new byte[]{}, new RecordHeaders(), period);

        when(scheduler.scheduleRecurrently(eq(period), any(FixedRateCommand.class))).thenReturn("123");

        sut.schedule(command);

        verify(scheduler).scheduleRecurrently(period, command);
    }
}
