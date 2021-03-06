package com.example;

import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.RowPutChange;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App
{
    public static void main( String[] args ) throws IOException, InterruptedException {
        String resourceCsvName = "csv/Crimes_-_2001_to_present_10000.csv";

        String tableName = "ots_chicago_crime";
        int primaryColNum = 0;

        TableStoreCsvParser parser = new CrimeParser(tableName, primaryColNum);
        TableStoreCsv csv = new TableStoreCsv(resourceCsvName, parser);

        // initial connection: 10 seconds
        // save 10000 rows: 15 seconds
//        TableStoreClient syncClient = new TableStoreSyncClient();
//        run(syncClient, csv);
//        syncClient.shutdown();

        // initial connection: 10 seconds
        // save 10000 rows: 5 seconds
        TableStoreAsyncCallback callback = new TableStoreAsyncCallback();
        TableStoreClient asyncClient = new TableStoreAsyncClient(callback);
        run(asyncClient, csv);
    }

    private static void run(TableStoreClient client, TableStoreCsv csv) throws InterruptedException {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

        int i = 1;
        int batchRequestNums = 200;     // upper limit: 200

        for (RowPutChange rowPutChange: csv) {
            batchWriteRowRequest.addRowChange(rowPutChange);

            // periodically submit request
            if (i % batchRequestNums == 0) {
                System.out.println("i: " + i);
                client.runBatchWriteRowRequest(batchWriteRowRequest);
                batchWriteRowRequest = new BatchWriteRowRequest();

                while (client.getCurrentRequestCount() > 50) {
                    System.out.println("sleep 1 sec.");
                    Thread.sleep(1000);
                }
            }
            i++;
        }

        if (!batchWriteRowRequest.isEmpty()) {
            client.runBatchWriteRowRequest(batchWriteRowRequest);
        }
    }
}
