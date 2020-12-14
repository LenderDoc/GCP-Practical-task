package rinftech.gcp.task;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class BigQueryService {
    
    private final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

    @Async("bigQueryExecutor")
    public void insertToTable(Iterable<InsertAllRequest.RowToInsert> rows, String dataset, String table) {
        TableId tableId = TableId.of(dataset, table);
        InsertAllRequest insertRequest = InsertAllRequest.newBuilder(tableId).setRows(rows).build();
        bigQuery.insertAll(insertRequest);
    }
}
