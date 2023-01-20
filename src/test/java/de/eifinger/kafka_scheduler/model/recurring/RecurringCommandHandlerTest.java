package de.eifinger.kafka_scheduler.model.recurring;

import de.eifinger.kafka_scheduler.kafka.producer.ReplyProducer;
import de.eifinger.kafka_scheduler.model.fixed_rate.FixedRateCommand;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class RecurringCommandHandlerTest {

    @Mock
    private ReplyProducer replyProducer;

    @Test
    void testCommandIsSend(){
        var key = "0815";
        var topic = "Topic";
        var targetKey = "uuid".getBytes(StandardCharsets.UTF_8);
        var value = "value".getBytes(StandardCharsets.UTF_8);
        var header = new RecordHeader("customHeader", "customHeader".getBytes(StandardCharsets.UTF_8));
        var headers = new RecordHeaders(List.of(header));
        var scheduleCommand = new RecurringCommand(key, topic, targetKey, value, headers, "cron");

        var expectedProducerRecord = new ProducerRecord<>(topic, targetKey,
                value);
        expectedProducerRecord.headers().add(header);

        var sut = new RecurringCommandHandler(replyProducer);
        sut.run(scheduleCommand);

        verify(replyProducer).send(expectedProducerRecord);
    }
}
