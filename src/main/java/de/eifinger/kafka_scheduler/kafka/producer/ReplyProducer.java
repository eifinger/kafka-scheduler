package de.eifinger.kafka_scheduler.kafka.producer;

import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.enterprise.context.ApplicationScoped;
import java.util.function.Consumer;

@ApplicationScoped
public class ReplyProducer {

    KafkaProducer<byte[], byte[]> producer;

    public ReplyProducer(KafkaClientService kafkaClientService) {
        this.producer = kafkaClientService.getProducer("outgoing");
    }

    public void send(ProducerRecord<byte[], byte[]> producerRecord) {
        producer.runOnSendingThread((Consumer<Producer<byte[], byte[]>>) client -> client.send(producerRecord))
                .await().indefinitely();
    }
}
