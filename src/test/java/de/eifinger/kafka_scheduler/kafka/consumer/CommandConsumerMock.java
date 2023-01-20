package de.eifinger.kafka_scheduler.kafka.consumer;

import de.eifinger.kafka_scheduler.model.command.SchedulerId;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class CommandConsumerMock extends GenericScheduleCommandConsumer {

    public static final SchedulerId SCHEDULED_ID = new SchedulerId("0815");
    @Override
    public void receive(ConsumerRecord<String, byte[]> record) {
        super.receive(record);
    }

    SchedulerId schedule(ConsumerRecord<String, byte[]> consumerRecord) {
        return SCHEDULED_ID;
    }

    void cancel(SchedulerId id) {}
}
