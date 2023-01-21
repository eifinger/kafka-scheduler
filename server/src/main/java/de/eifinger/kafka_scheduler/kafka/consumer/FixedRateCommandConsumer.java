package de.eifinger.kafka_scheduler.kafka.consumer;

import de.eifinger.kafka_scheduler.model.command.SchedulerId;
import de.eifinger.kafka_scheduler.model.fixed_rate.FixedRateCommandMapper;
import de.eifinger.kafka_scheduler.model.fixed_rate.FixedRateCommandScheduler;
import io.smallrye.common.annotation.Identifier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
@Identifier("fixed-rate-command-scheduler")
public class FixedRateCommandConsumer extends GenericScheduleCommandConsumer {

    @Inject
    FixedRateCommandScheduler commandScheduler;
    @Inject
    FixedRateCommandMapper commandMapper;

    @Incoming("fixed-rate")
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
