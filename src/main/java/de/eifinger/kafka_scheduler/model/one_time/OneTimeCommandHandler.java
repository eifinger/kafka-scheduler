package de.eifinger.kafka_scheduler.model.one_time;

import de.eifinger.kafka_scheduler.kafka.producer.CommandDeletionProducer;
import de.eifinger.kafka_scheduler.kafka.producer.ReplyProducer;
import de.eifinger.kafka_scheduler.model.command.CommandCompletionHandler;
import de.eifinger.kafka_scheduler.model.command.AbstractCommandHandler;
import io.quarkus.arc.Unremovable;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jobrunr.jobs.lambdas.JobRequestHandler;
import org.slf4j.Logger;

import javax.enterprise.context.Dependent;

@Dependent
@Unremovable
public class OneTimeCommandHandler extends AbstractCommandHandler implements JobRequestHandler<OneTimeCommand> {

    private final CommandCompletionHandler commandCompletionHandler;
    private final CommandDeletionProducer commandDeletionProducer;
    private final Logger log = org.slf4j.LoggerFactory.getLogger(getClass());

    private final String oneTimeSchedulerTopic;

    protected OneTimeCommandHandler(
            ReplyProducer replyProducer,
            CommandCompletionHandler commandCompletionHandler,
            CommandDeletionProducer commandDeletionProducer,
            @ConfigProperty(name = "mp.messaging.incoming.one-time.topic") String oneTimeSchedulerTopic) {
        super(replyProducer);
        this.commandCompletionHandler = commandCompletionHandler;
        this.commandDeletionProducer = commandDeletionProducer;
        this.oneTimeSchedulerTopic = oneTimeSchedulerTopic;
    }

    @Override
    public void run(OneTimeCommand command) {
        try {
            sendCommand(command);

            deleteScheduleCommand(command);

            this.commandCompletionHandler.commandCompleted(command);
        } catch (Exception exception) {
            log.error("Error while trying to send command. Will not delete ScheduleCommand", exception);
        }

    }

    /**
     * Send a tombstone message deleting the ScheduleCommand from the topic.
     */
    private void deleteScheduleCommand(OneTimeCommand command) {
        ProducerRecord<String, Void> scheduleCommandDeletionProducerRecord =
                new ProducerRecord<>(oneTimeSchedulerTopic, command.id(), null);
        log.info("Deleting ScheduleCommand: {}", command.id());
        this.commandDeletionProducer.send(scheduleCommandDeletionProducerRecord);
    }
}
