package com.github.ajablonski;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;
import java.util.Objects;

@Configuration
@EnableKafkaStreams
public class StreamsConfiguration {
    @Bean
    public KTable<Windowed<String>, String> buildWindowedAggregationWithForeignKeyJoin(StreamsBuilder streamsBuilder) {
        var inputTopic = streamsBuilder.<String, Integer>stream("WordCounts", Consumed.with(Serdes.String(), Serdes.Integer()));
        var wordsForNumbers = streamsBuilder.<Integer, String>table("WordsForNumbers");

        Duration windowSize = Duration.ofMinutes(10);

        var windowedStringKTable = inputTopic
                .groupBy(
                        (k, v) -> k.toUpperCase().substring(0, 1),
                        Grouped.with("GroupName", Serdes.String(), Serdes.Integer())
                )
                .windowedBy(TimeWindows.of(windowSize))
                .aggregate(
                        () -> 0,
                        (k, v, sumSoFar) -> sumSoFar + v,
                        Materialized.with(Serdes.String(), Serdes.Integer()))
                .leftJoin(
                        wordsForNumbers,
                        (v) -> v,
                        (count, wordForCount) -> Objects.requireNonNullElseGet(wordForCount, count::toString)
                )
                .mapValues((k, v) -> String.format("There were %s words starting with %s between %s and %s", v, k.key(), k.window().startTime(), k.window().endTime()));

        windowedStringKTable.toStream().to("OutputTopic");

        return windowedStringKTable;
    }
}
