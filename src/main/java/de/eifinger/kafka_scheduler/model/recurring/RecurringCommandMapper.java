package de.eifinger.kafka_scheduler.model.recurring;

import de.eifinger.kafka_scheduler.Util;
import de.eifinger.kafka_scheduler.model.command.CommandMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.KEY;
import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.TOPIC;
import static de.eifinger.kafka_scheduler.model.recurring.RecurringCommand.CRON;

@ApplicationScoped
public class RecurringCommandMapper implements CommandMapper<RecurringCommand> {
    @Override
    public RecurringCommand toScheduleCommand(ConsumerRecord<String, byte[]> consumerRecord) {
        var headers = consumerRecord.headers();
        var topic = new String(headers.lastHeader(TOPIC).value(), StandardCharsets.UTF_8);
        var cron = new String(headers.lastHeader(CRON).value(), StandardCharsets.UTF_8);
        var key = headers.lastHeader(KEY).value();
        var headersWithout = Util.headersWithout(headers, List.of(TOPIC, KEY, CRON));
        return new RecurringCommand(consumerRecord.key(), topic, key, consumerRecord.value(), headersWithout, cron);
    }
}
