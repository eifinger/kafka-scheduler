package de.eifinger.kafka.scheduler;

import io.smallrye.common.annotation.Identifier;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
@Identifier("consumer")
public class ConsoleLogConsumer {

    private final Logger log = org.slf4j.LoggerFactory.getLogger(getClass());

    @Incoming("consumer")
    public void receive(Payload payload) {
        log.info(payload.name());
    }
}
