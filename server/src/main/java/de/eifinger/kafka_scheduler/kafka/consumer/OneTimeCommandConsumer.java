package de.eifinger.kafka_scheduler.kafka.consumer;

import de.eifinger.kafka_scheduler.model.command.CommandCompletionHandler;
import de.eifinger.kafka_scheduler.model.command.SchedulerId;
import de.eifinger.kafka_scheduler.model.one_time.OneTimeCommandMapper;
import de.eifinger.kafka_scheduler.model.one_time.OneTimeCommandScheduler;
import io.smallrye.common.annotation.Identifier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import javax.inject.Inject;

@ApplicationScoped
@Identifier("one-time-command-scheduler")
@Default
public class OneTimeCommandConsumer extends GenericScheduleCommandConsumer implements CommandCompletionHandler {

    @Inject
    OneTimeCommandScheduler commandScheduler;
    @Inject
    OneTimeCommandMapper commandMapper;

    @Incoming("one-time")
    @Override
    public void receive(ConsumerRecord<String, byte[]> consumerRecord) {
        super.receive(consumerRecord);
    }

    SchedulerId schedule(ConsumerRecord<String, byte[]> consumerRecord) {
        var command = commandMapper.toScheduleCommand(consumerRecord);
        return commandScheduler.schedule(command);
    }

    void cancel(SchedulerId schedulerId) {
        commandScheduler.cancel(schedulerId);
    }
}
