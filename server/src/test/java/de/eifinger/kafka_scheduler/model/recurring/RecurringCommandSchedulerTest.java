package de.eifinger.kafka_scheduler.model.recurring;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecurringCommandSchedulerTest {

    @InjectMocks
    RecurringCommandScheduler sut;
    @Mock
    JobRequestScheduler scheduler;

    @Test
    void testCommandIsScheduled(){
        var cron = "5 4 * * *";
        var command = new RecurringCommand("id", "topic", new byte[]{}, new byte[]{}, new RecordHeaders(), cron);

        when(scheduler.scheduleRecurrently(eq(cron), any(RecurringCommand.class))).thenReturn("123");

        sut.schedule(command);

        verify(scheduler).scheduleRecurrently(cron, command);
    }
}
