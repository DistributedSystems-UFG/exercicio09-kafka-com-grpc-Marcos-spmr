import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import java.time.Duration;
import java.util.*;

public class DataProcessor {
    private static final String TOPIC_IN = "sensor-temperatura-bruta";
    private static final String TOPIC_OUT = "sensor-temperatura-processada";

    public static void main(String[] args) {
        Properties consProps = new Properties();
        consProps.put("bootstrap.servers", "localhost:9092");
        consProps.put("group.id", "grupo-processador");
        consProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consProps.put("value.deserializer", "org.apache.kafka.common.serialization.DoubleDeserializer");
        KafkaConsumer<String, Double> consumer = new KafkaConsumer<>(consProps);
        consumer.subscribe(Collections.singletonList(TOPIC_IN));

        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", "localhost:9092");
        prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodProps.put("value.serializer", "org.apache.kafka.common.serialization.DoubleSerializer");
        KafkaProducer<String, Double> producer = new KafkaProducer<>(prodProps);

        List<Double> buffer = new ArrayList<>();

        System.out.println("Processador iniciado...");
        while (true) {
            ConsumerRecords<String, Double> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Double> record : records) {
                buffer.add(record.value());
                
                if (buffer.size() > 10) buffer.remove(0);

                if (buffer.size() == 10) {
                    double media = buffer.stream().mapToDouble(val -> val).average().orElse(0.0);
                    producer.send(new ProducerRecord<>(TOPIC_OUT, "media-sensor-1", media));
                    System.out.printf("Média calculada e publicada: %.2f°C%n", media);
                }
            }
        }
    }
}