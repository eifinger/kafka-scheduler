package de.eifinger.kafka.scheduler;

import de.eifinger.kafka.scheduler.client.KafkaSchedulerClient;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.text.MessageFormat;
import java.time.LocalDateTime;

@Path("/scheduler")
public class SchedulerResource {

    @Inject
    KafkaSchedulerClient<String, Payload> client;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        return "Hello from RESTEasy Reactive";
    }

    @POST
    @Path("one-time")
    @Produces(MediaType.TEXT_PLAIN)
    public String oneTime() {
        var id = client.schedule("myKey", new Payload("hello"), "example-quarkus-consumer", LocalDateTime.now());
        return MessageFormat.format("Scheduled {0}", id);
    }

    @POST
    @Path("recurring")
    @Produces(MediaType.TEXT_PLAIN)
    public String recurring() {
        var id = client.schedule("myKey", new Payload("hello"), "example-quarkus-consumer", LocalDateTime.now());
        return MessageFormat.format("Scheduled {0}", id);
    }
}