package de.eifinger.kafka_scheduler.model.command;

import de.eifinger.kafka_scheduler.kafka.producer.ReplyProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

public abstract class AbstractCommandHandler {

    protected final ReplyProducer replyProducer;
    private final Logger log = org.slf4j.LoggerFactory.getLogger(getClass());

    protected AbstractCommandHandler(ReplyProducer replyProducer) {
        this.replyProducer = replyProducer;
    }

    /**
     * Send the payload/command which was defined in the ScheduleCommand to the topic which was defined in the
     * ScheduleCommand.
     */
    protected void sendCommand(ScheduleCommand command) {
        var producerRecord =
                new ProducerRecord<>(command.topic(), command.key(),
                        command.value());
        command.headers().forEach(producerRecord.headers()::add);
        log.info("Sending command with id: {} on topic: {}", command.id(),
                command.topic());
        this.replyProducer.send(producerRecord);
    }
}
