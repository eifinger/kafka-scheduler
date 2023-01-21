package de.eifinger.kafka_scheduler.model.fixed_rate;

import de.eifinger.kafka_scheduler.model.command.AbstractCommandScheduler;
import de.eifinger.kafka_scheduler.model.command.CommandScheduler;
import de.eifinger.kafka_scheduler.model.command.SchedulerId;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.slf4j.Logger;

import javax.inject.Singleton;

@Singleton
public class FixedRateCommandScheduler extends AbstractCommandScheduler implements CommandScheduler<FixedRateCommand> {

    private final Logger log = org.slf4j.LoggerFactory.getLogger(getClass());

    public FixedRateCommandScheduler(JobRequestScheduler jobRequestScheduler) {
        super(jobRequestScheduler);
    }

    public SchedulerId schedule(
            FixedRateCommand command) {
        var id = jobRequestScheduler.scheduleRecurrently(command.period(), command);
        log.info("OneTimeCommand {} scheduled with id {}", command.id(), id);
        return new SchedulerId(id);
    }
}
