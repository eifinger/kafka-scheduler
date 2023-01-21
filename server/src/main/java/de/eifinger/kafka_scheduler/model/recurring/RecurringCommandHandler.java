package de.eifinger.kafka_scheduler.model.recurring;

import de.eifinger.kafka_scheduler.kafka.producer.ReplyProducer;
import de.eifinger.kafka_scheduler.model.command.AbstractCommandHandler;
import io.quarkus.arc.Unremovable;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

import javax.enterprise.context.Dependent;

@Dependent
@Unremovable
public class RecurringCommandHandler extends AbstractCommandHandler implements JobRequestHandler<RecurringCommand> {

    protected RecurringCommandHandler(
            ReplyProducer replyProducer
    ) {
        super(replyProducer);
    }

    @Override
    public void run(RecurringCommand command) {
        sendCommand(command);
    }
}
