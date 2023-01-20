package de.eifinger.kafka_scheduler.model.fixed_rate;

import de.eifinger.kafka_scheduler.Util;
import de.eifinger.kafka_scheduler.model.command.CommandMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.KEY;
import static de.eifinger.kafka_scheduler.model.command.ScheduleCommandHeaders.TOPIC;
import static de.eifinger.kafka_scheduler.model.fixed_rate.FixedRateCommand.PERIOD;

@Singleton
public class FixedRateCommandMapper implements CommandMapper<FixedRateCommand> {
    @Override
    public FixedRateCommand toScheduleCommand(ConsumerRecord<String, byte[]> consumerRecord) {
        var headers = consumerRecord.headers();

        var topic = new String(headers.lastHeader(TOPIC).value(), StandardCharsets.UTF_8);

        var key = headers.lastHeader(KEY).value();

        var periodString = new String(headers.lastHeader(PERIOD).value(), StandardCharsets.UTF_8);
        var period = Duration.parse(periodString);

        var headersWithout = Util.headersWithout(headers, List.of(TOPIC, KEY, PERIOD));

        return new FixedRateCommand(consumerRecord.key(), topic, key, consumerRecord.value(), headersWithout, period);
    }
}
