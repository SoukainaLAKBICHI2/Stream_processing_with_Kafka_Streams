package org.app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class AnalyseMeteo {

    public static void main(String[] args) {

        // Configuration de Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-processing-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Liste des mots interdits
        List<String> forbiddenWords = Arrays.asList("HACK", "SPAM", "XXX");

        StreamsBuilder builder = new StreamsBuilder();

        // Lecture du topic text-input
        KStream<String, String> inputStream = builder.stream("text-input");

        // Traitement
        KStream<String, String>[] branches = inputStream
                .mapValues(value -> {
                    if (value == null) return "";
                    // Nettoyage du texte
                    String cleaned = value.trim().replaceAll("\\s+", " ").toUpperCase();
                    return cleaned;
                })
                .branch(
                        // Branche 0 : messages valides
                        (key, value) -> isValid(value, forbiddenWords),
                        // Branche 1 : messages invalides
                        (key, value) -> true
                );

        // Routage
        KStream<String, String> validStream = branches[0];
        KStream<String, String> invalidStream = branches[1];

        validStream.to("text-clean");
        invalidStream.to("text-dead-letter");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Shutdown hook pour arrêter proprement
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static boolean isValid(String value, List<String> forbiddenWords) {
        if (value == null || value.isBlank()) return false;
        if (value.length() > 100) return false;
        for (String word : forbiddenWords) {
            if (value.contains(word)) return false;
        }
        return true;
    }
}
