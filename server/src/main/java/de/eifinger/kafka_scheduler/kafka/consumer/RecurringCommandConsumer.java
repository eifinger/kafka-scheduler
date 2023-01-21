package de.eifinger.kafka_scheduler.kafka.consumer;

import de.eifinger.kafka_scheduler.model.command.SchedulerId;
import de.eifinger.kafka_scheduler.model.recurring.RecurringCommandMapper;
import de.eifinger.kafka_scheduler.model.recurring.RecurringCommandScheduler;
import io.smallrye.common.annotation.Identifier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
@Identifier("recurring-command-scheduler")
public class RecurringCommandConsumer extends GenericScheduleCommandConsumer {

    @Inject
    RecurringCommandScheduler commandScheduler;
    @Inject
    RecurringCommandMapper commandMapper;

    @Incoming("recurring")
    @Override
    public void receive(ConsumerRecord<String, byte[]> consumerRecord) {
        super.receive(consumerRecord);
    }

    SchedulerId schedule(ConsumerRecord<String, byte[]> consumerRecord) {
        var command = commandMapper.toScheduleCommand(consumerRecord);
        return commandScheduler.schedule(command);
    }

    void cancel(SchedulerId id) {
        commandScheduler.cancel(id);
    }
}
