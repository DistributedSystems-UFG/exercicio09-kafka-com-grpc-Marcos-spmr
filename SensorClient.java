import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import br.ufg.inf.sd.grpc.*;

public class SensorClient {
    public static void main(String[] args) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        SensorServiceGrpc.SensorServiceBlockingStub stub = SensorServiceGrpc.newBlockingStub(channel);

        EmptyRequest request = EmptyRequest.newBuilder().build();

        System.out.println("--- Consultando Última Média ---");
        SensorData latest = stub.getLatestAverage(request);
        System.out.printf("Temperatura: %.2f°C | Timestamp: %d%n", latest.getTemperature(), latest.getTimestamp());

        System.out.println("\n--- Consultando Histórico ---");
        HistoryResponse history = stub.getHistory(request);
        for (SensorData data : history.getRecordsList()) {
            System.out.printf("- %.2f°C em %d%n", data.getTemperature(), data.getTimestamp());
        }

        channel.shutdown();
    }
}