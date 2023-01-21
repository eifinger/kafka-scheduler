package de.eifinger.kafka_scheduler.model.fixed_rate;

import de.eifinger.kafka_scheduler.kafka.producer.ReplyProducer;
import de.eifinger.kafka_scheduler.model.command.AbstractCommandHandler;
import io.quarkus.arc.Unremovable;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

import javax.enterprise.context.Dependent;

@Dependent
@Unremovable
public class FixedRateCommandHandler extends AbstractCommandHandler implements JobRequestHandler<FixedRateCommand> {

    protected FixedRateCommandHandler(
            ReplyProducer replyProducer) {
        super(replyProducer);
    }

    @Override
    public void run(FixedRateCommand command) {
        sendCommand(command);
    }
}
