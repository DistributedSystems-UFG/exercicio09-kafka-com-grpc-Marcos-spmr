import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.consumer.*;
import br.ufg.inf.sd.grpc.*;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class StorageWebService {
    private static final List<SensorData> database = new CopyOnWriteArrayList<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        Thread kafkaThread = new Thread(StorageWebService::consumeKafkaEvents);
        kafkaThread.start();

        Server server = ServerBuilder.forPort(50051)
                .addService(new SensorServiceImpl())
                .build()
                .start();

        System.out.println("Servidor gRPC iniciado na porta 50051...");
        server.awaitTermination();
    }

    private static void consumeKafkaEvents() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "grupo-storage");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.DoubleDeserializer");
        
        KafkaConsumer<String, Double> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("sensor-temperatura-processada"));

        while (true) {
            ConsumerRecords<String, Double> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Double> record : records) {
                SensorData data = SensorData.newBuilder()
                        .setTemperature(record.value())
                        .setTimestamp(System.currentTimeMillis())
                        .build();
                database.add(data);
                System.out.println("Dado persistido no BD: " + record.value());
            }
        }
    }

    static class SensorServiceImpl extends SensorServiceGrpc.SensorServiceImplBase {
        @Override
        public void getLatestAverage(EmptyRequest req, StreamObserver<SensorData> responseObserver) {
            if (database.isEmpty()) {
                responseObserver.onNext(SensorData.newBuilder().build());
            } else {
                responseObserver.onNext(database.get(database.size() - 1));
            }
            responseObserver.onCompleted();
        }

        @Override
        public void getHistory(EmptyRequest req, StreamObserver<HistoryResponse> responseObserver) {
            HistoryResponse response = HistoryResponse.newBuilder()
                    .addAllRecords(database)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}