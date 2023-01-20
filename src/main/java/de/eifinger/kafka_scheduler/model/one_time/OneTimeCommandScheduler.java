package de.eifinger.kafka_scheduler.model.one_time;

import de.eifinger.kafka_scheduler.model.command.AbstractCommandScheduler;
import de.eifinger.kafka_scheduler.model.command.CommandScheduler;
import de.eifinger.kafka_scheduler.model.command.SchedulerId;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.slf4j.Logger;

import javax.enterprise.context.Dependent;

@Dependent
public class OneTimeCommandScheduler extends AbstractCommandScheduler implements CommandScheduler<OneTimeCommand> {

    private final Logger log = org.slf4j.LoggerFactory.getLogger(getClass());

    public OneTimeCommandScheduler(JobRequestScheduler jobRequestScheduler) {
        super(jobRequestScheduler);
    }

    public SchedulerId schedule(OneTimeCommand command) {
        var id = jobRequestScheduler.schedule(command.when(), command);
        log.info("OneTimeCommand {} scheduled with id {}", command.id(), id);
        return new SchedulerId(id);
    }
}
