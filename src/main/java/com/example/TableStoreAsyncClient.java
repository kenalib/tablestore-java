package com.example;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;

import java.util.HashMap;
import java.util.Map;

class TableStoreAsyncClient extends TableStoreClient {
    private TableStoreCallback<BatchWriteRowRequest, BatchWriteRowResponse> callback;
    private AsyncClient client;
    private Map<Integer, BatchWriteRowRequest> requested = new HashMap<Integer, BatchWriteRowRequest>();

    TableStoreAsyncClient(TableStoreAsyncCallback callback) {
        this.callback = callback;
        callback.setAsyncClient(this);

        readAccountInfo();
        createClientConfiguration();

        client = new AsyncClient(endPoint, accessKeyId, accessKeySecret, instanceName, clientConfiguration);
    }

    void shutdown() {
        System.out.println("client shutdown...");
        client.shutdown();
    }

    void batchWriteRow(BatchWriteRowRequest request) {
        client.batchWriteRow(request, callback);
    }

    void removeRequested(int hashCode) {
        requested.remove(hashCode);
    }

    int getCurrentRequestCount() {
        return requested.size();
    }

    void runBatchWriteRowRequest(final BatchWriteRowRequest batchWriteRowRequest) {
        System.out.println("Running Batch: " + batchWriteRowRequest.getRowsCount());
        requested.put(batchWriteRowRequest.hashCode(), batchWriteRowRequest);

        client.batchWriteRow(batchWriteRowRequest, callback);
    }
}
