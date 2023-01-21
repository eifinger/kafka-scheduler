package de.eifinger.kafka_scheduler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class UtilTest {

    @ParameterizedTest
    @MethodSource("provideParameters")
    void test_calculateInitialDelay(LocalDateTime now, LocalDateTime when, Duration period, Duration expected){
        var result = Util.calculateInitialDelay(now, when, period);
        assertThat(result).isEqualTo(expected);
    }

    private static Stream<Arguments> provideParameters() {
        return Stream.of(
                Arguments.of(
                        LocalDateTime.of(2022, 11, 28, 11, 0),
                        LocalDateTime.of(2022, 11, 28, 12, 0),
                        Duration.ofHours(2),
                        Duration.ofHours(1)),
                Arguments.of(
                        LocalDateTime.of(2022, 11, 28, 15, 0),
                        LocalDateTime.of(2022, 11, 28, 12, 0),
                        Duration.ofHours(2),
                        Duration.ofHours(1)),
                Arguments.of(
                        LocalDateTime.of(2022, 11, 28, 10, 0),
                        LocalDateTime.of(2022, 11, 30, 12, 0),
                        Duration.ofHours(2),
                        Duration.ofHours(2).plusDays(2))
        );
    }
}
