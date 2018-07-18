package com.example;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;

import java.util.HashMap;
import java.util.Map;

class TableStoreSyncClient extends TableStoreClient {
    private SyncClient client;
    private volatile Map<Integer, BatchWriteRowRequest> requested = new HashMap<Integer, BatchWriteRowRequest>();

    TableStoreSyncClient() {
        readAccountInfo();
        createClientConfiguration();

        client = new SyncClient(endPoint, accessKeyId, accessKeySecret, instanceName, clientConfiguration);
    }

    void shutdown() {
        client.shutdown();
    }

    int getCurrentRequestCount() {
        return requested.size();
    }

    void runBatchWriteRowRequest(BatchWriteRowRequest batchWriteRowRequest) {
        System.out.println("Running Batch: " + batchWriteRowRequest.getRowsCount());
        requested.put(batchWriteRowRequest.hashCode(), batchWriteRowRequest);

        BatchWriteRowResponse response = client.batchWriteRow(batchWriteRowRequest);

        if (response.isAllSucceed()) {
            System.out.println("All Succeed rows: " + response.getSucceedRows().size());
            requested.remove(batchWriteRowRequest.hashCode());
        } else {
            for (BatchWriteRowResponse.RowResult rowResult : response.getFailedRows()) {
                System.out.println("Failed rows:" + batchWriteRowRequest.getRowChange(rowResult.getTableName(), rowResult.getIndex()).getPrimaryKey());
                System.out.println("Cause of failure:" + rowResult.getError());
            }
            /*
             * You can use the createRequestForRetry method
             * to construct another request to retry failed rows.
             * Here, we only construct part of the retry request.
             * For the retry method, we recommend using the SDK's custom retry policy function.
             * This function allows you to retry failed rows after batch operations.
             * After setting the retry policy, you do not need to add retry code to the calling API.
             */
            BatchWriteRowRequest retryRequest = batchWriteRowRequest.createRequestForRetry(response.getFailedRows());
            System.out.println("retryRequest count: " + retryRequest.getRowsCount());
        }
    }
}
