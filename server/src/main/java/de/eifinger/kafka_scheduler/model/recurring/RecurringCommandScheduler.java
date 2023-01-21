package de.eifinger.kafka_scheduler.model.recurring;

import de.eifinger.kafka_scheduler.model.command.AbstractCommandScheduler;
import de.eifinger.kafka_scheduler.model.command.CommandScheduler;
import de.eifinger.kafka_scheduler.model.command.SchedulerId;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.slf4j.Logger;

import javax.enterprise.context.Dependent;

@Dependent
public class RecurringCommandScheduler extends AbstractCommandScheduler implements CommandScheduler<RecurringCommand> {

    private final Logger log = org.slf4j.LoggerFactory.getLogger(getClass());

    public RecurringCommandScheduler(JobRequestScheduler jobRequestScheduler) {
        super(jobRequestScheduler);
    }

    public SchedulerId schedule(RecurringCommand command) {
        var id = jobRequestScheduler.scheduleRecurrently(command.cron(), command);
        log.info("RecurringCommandTask {} scheduled with id {}", command.id(), id);
        return new SchedulerId(id);
    }
}
