import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Random;

public class SensorProducer {
    private static final String TOPIC = "sensor-temperatura-bruta";

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.DoubleSerializer");

        KafkaProducer<String, Double> producer = new KafkaProducer<>(props);
        Random random = new Random();
        double baseTemp = 25.0;

        System.out.println("Sensor iniciado. Emitindo eventos...");
        while (true) {
            double temp = baseTemp + (random.nextDouble() * 4 - 2); 
            ProducerRecord<String, Double> record = new ProducerRecord<>(TOPIC, "sensor-1", temp);
            
            producer.send(record);
            System.out.printf("Enviado: %.2f°C%n", temp);
            
            Thread.sleep(2000);
        }
    }
}