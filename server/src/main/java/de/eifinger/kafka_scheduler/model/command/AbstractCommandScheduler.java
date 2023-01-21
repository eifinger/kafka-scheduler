package de.eifinger.kafka_scheduler.model.command;

import org.jobrunr.scheduling.JobRequestScheduler;

public abstract class AbstractCommandScheduler {

    protected JobRequestScheduler jobRequestScheduler;

    protected AbstractCommandScheduler(JobRequestScheduler jobRequestScheduler) {
        this.jobRequestScheduler = jobRequestScheduler;
    }

    public void cancel(SchedulerId schedulerId) {
        // If delete is called with a String the job won't be cancelled only future invocation won't happen
        jobRequestScheduler.delete(schedulerId.toJobId());
    }
}
