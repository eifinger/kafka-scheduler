package de.eifinger.kafka_scheduler.model.one_time;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jobrunr.jobs.JobId;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OneTimeCommandSchedulerTest {

    private final static LocalDate LOCAL_DATE = LocalDate.of(1990, 7, 20);
    private static final Clock fixedClock = Clock.fixed(LOCAL_DATE.atStartOfDay(ZoneId.systemDefault()).toInstant(), ZoneId.systemDefault());

    @InjectMocks
    private OneTimeCommandScheduler sut;
    @Mock
    private JobRequestScheduler scheduler;
    @Mock
    private Clock clock;

    @Test
    void testCommandIsScheduled(){
        when(clock.instant()).thenReturn(fixedClock.instant());
        when(clock.getZone()).thenReturn(fixedClock.getZone());
        var when = LocalDateTime.now(clock);
        var command = new OneTimeCommand("id", "topic", new byte[]{}, new byte[]{}, new RecordHeaders(), when);

        when(scheduler.schedule(any(LocalDateTime.class), any(OneTimeCommand.class))).thenReturn(new JobId(UUID.randomUUID()));

        sut.schedule(command);

        verify(scheduler).schedule(when, command);
    }
}
