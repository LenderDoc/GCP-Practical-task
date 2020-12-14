package rinftech.gcp.task;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import example.gcp.Client;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GcpTask {

    @Value("${bigquery.dataset}")
    private String datasetName;

    @Value("${bigquery.table.clients}")
    private String clientsTableName;

    @Value("${bigquery.table.clientssubset}")
    private String clientsSubsetTableName;

    @Autowired
    private BigQueryService bigQueryService;
    private final Storage storage = StorageOptions.getDefaultInstance().getService();

    public void StreamToBigQuery(String bucketId, String objectId) {
        Blob blob = storage.get(BlobId.of(bucketId, objectId));
        List<Client> clients;

        try {
            clients = DeserializeClients(blob.getContent());
        } catch (IOException e) {
            System.out.println("An IO Exception while deserialization. " + e.getMessage());
            return;
        }

        List<InsertAllRequest.RowToInsert> rowsOfClients = clients.stream().map(c -> {
            Map<String, Object> rowContent = new HashMap<>();
            rowContent.put("id", c.getId());
            rowContent.put("name", c.getName().toString());

            if (c.getPhone() != null) {
                rowContent.put("phone", c.getPhone().toString());
            }
            if (c.getAddress() != null) {
                rowContent.put("address", c.getAddress().toString());
            }
            return InsertAllRequest.RowToInsert.of(rowContent);
        }).collect(Collectors.toList());

        List<InsertAllRequest.RowToInsert> rowsOfClientsSubset = clients.stream().map(c -> {
            Map<String, Object> rowContent = new HashMap<>();
            rowContent.put("id", c.getId());
            rowContent.put("name", c.getName().toString());
            return InsertAllRequest.RowToInsert.of(rowContent);
        }).collect(Collectors.toList());

        bigQueryService.insertToTable(rowsOfClients, datasetName, clientsTableName);
        bigQueryService.insertToTable(rowsOfClientsSubset, datasetName, clientsSubsetTableName);
    }

    public static List<Client> DeserializeClients(byte[] data) throws IOException {
        List<Client> clients = new ArrayList<>();
        DatumReader<Client> clientsDatumReader = new SpecificDatumReader<>(Client.class);

        try ( InputStream clientsInputStream = new ByteArrayInputStream(data);  DataFileStream<Client> dataFileReader = new DataFileStream<>(clientsInputStream, clientsDatumReader)) {
            while (dataFileReader.hasNext()) {
                clients.add(dataFileReader.next());
            }
        }
        return clients;
    }
}
