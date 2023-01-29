package de.eifinger.kafka.scheduler.client;

import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.enterprise.context.Dependent;
import java.util.function.Consumer;

@Dependent
public class QuarkusAdapter<K, V> implements KafkaProducer<K, V>{

    io.smallrye.reactive.messaging.kafka.KafkaProducer<K, V> kafkaProducer;

    public QuarkusAdapter(KafkaClientService clientService) {
        this.kafkaProducer = clientService.getProducer("outgoing");
    }
    @Override
    public void send(ProducerRecord<K, V> producerRecord) {
        kafkaProducer.runOnSendingThread((Consumer<Producer<K, V>>) client -> client.send(producerRecord))
                .await().indefinitely();
    }
}
