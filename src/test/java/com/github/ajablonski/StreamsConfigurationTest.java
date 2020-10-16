package com.github.ajablonski;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        properties = {
                "spring.application.name=demo-application",
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
        },
        classes = StreamsConfiguration.class
)
@EnableAutoConfiguration
@EmbeddedKafka(
        topics = {
                "WordCounts", "WordsForNumbers", "OutputTopic"
        }
)
class StreamsConfigurationTest {
    @Autowired
    private StreamsBuilderFactoryBean streamsBuilder;

    @Value("${spring.embedded.kafka.brokers}")
    private String embeddedKafkaUrl;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @Test
    public void shouldTransitionToRunningStateBeforeTimeout() throws InterruptedException {
        var startTime = LocalDateTime.now();
        var timeout = Duration.ofSeconds(30);

        KafkaStreams.State state;
        do {
            state = streamsBuilder.getKafkaStreams().state();
            System.out.printf("State is %s%n", state);
            Thread.sleep(1000);
        } while (state != KafkaStreams.State.RUNNING && LocalDateTime.now().isBefore(startTime.plus(timeout)));

        assertThat(state).isEqualTo(KafkaStreams.State.RUNNING);
    }

    @Test
    public void shouldProcessData() {
        var stringIntegerKafkaTemplate = buildStringIntKafkaTemplate();
        var integerStringKafkaTemplate = buildIntStringKafkaTemplate();

        var consumer = consumerFactory.createConsumer("groupId", "suffix");
        consumer.subscribe(Lists.newArrayList("OutputTopic"));

        stringIntegerKafkaTemplate.send("WordCounts", "welcome", 1);
        stringIntegerKafkaTemplate.send("WordCounts", "everything", 1);
        stringIntegerKafkaTemplate.send("WordCounts", "is", 1);
        stringIntegerKafkaTemplate.send("WordCounts", "fine", 12);
        integerStringKafkaTemplate.send("WordsForNumbers", 1, "one");

        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer);
        assertThat(records).isNotEmpty();
    }

    private KafkaTemplate<String, Integer> buildStringIntKafkaTemplate() {
        return new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(
                        new HashMap<>() {{
                            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaUrl);
                            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
                            put(ProducerConfig.CLIENT_ID_CONFIG, "WordCountProducer");
                        }}
                ));
    }

    private KafkaTemplate<Integer, String> buildIntStringKafkaTemplate() {
        return new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(
                        new HashMap<>() {{
                            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaUrl);
                            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
                            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                            put(ProducerConfig.CLIENT_ID_CONFIG, "NumberNamesProducer");
                        }}
                )
        );
    }
}