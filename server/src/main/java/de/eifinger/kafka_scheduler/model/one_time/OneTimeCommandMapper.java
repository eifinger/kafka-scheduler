package de.eifinger.kafka_scheduler.model.one_time;

import de.eifinger.kafka_scheduler.Util;
import de.eifinger.kafka_scheduler.model.command.CommandMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.KEY;
import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.TOPIC;
import static de.eifinger.kafka_scheduler.model.one_time.OneTimeCommand.WHEN;

@Singleton
public class OneTimeCommandMapper implements CommandMapper<OneTimeCommand> {
    @Override
    public OneTimeCommand toScheduleCommand(ConsumerRecord<String, byte[]> consumerRecord) {
        var headers = consumerRecord.headers();
        var topic = new String(headers.lastHeader(TOPIC).value(), StandardCharsets.UTF_8);
        var whenString = new String(headers.lastHeader(WHEN).value(), StandardCharsets.UTF_8);
        var when = LocalDateTime.parse(whenString, DateTimeFormatter.ISO_DATE_TIME);
        var key = headers.lastHeader(KEY).value();
        var headersWithout = Util.headersWithout(headers, List.of(TOPIC, KEY, WHEN));
        return new OneTimeCommand(consumerRecord.key(), topic, key, consumerRecord.value(), headersWithout, when);
    }
}
