package de.eifinger.kafka_scheduler.kafka.producer;

import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.enterprise.context.ApplicationScoped;
import java.util.function.Consumer;

@ApplicationScoped
public class CommandDeletionProducer {

    KafkaProducer<String, Void> producer;

    public CommandDeletionProducer(KafkaClientService kafkaClientService) {
        this.producer = kafkaClientService.getProducer("one-time-out");
    }

    public void send(ProducerRecord<String, Void> producerRecord) {
        producer.runOnSendingThread((Consumer<Producer<String, Void>>) client -> client.send(producerRecord))
                .await().indefinitely();
    }
}
