package com.example;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;

class TableStoreAsyncClient extends TableStoreClient {
    private AsyncClient client;

    TableStoreAsyncClient() {
        readAccountInfo();
        createClientConfiguration();

        client = new AsyncClient(endPoint, accessKeyId, accessKeySecret, instanceName, clientConfiguration);
    }

    void shutdown() {
        client.shutdown();
    }

    void runBatchWriteRowRequest(final BatchWriteRowRequest batchWriteRowRequest) {
        System.out.println("Running Batch: " + batchWriteRowRequest.getRowsCount());

        TableStoreCallback<BatchWriteRowRequest, BatchWriteRowResponse> callback = new TableStoreCallback<BatchWriteRowRequest, BatchWriteRowResponse>() {
            public void onCompleted(BatchWriteRowRequest request, BatchWriteRowResponse response) {
                int succeedRows = response.getSucceedRows().size();
                if (response.isAllSucceed()) {
                    System.out.println("AllSucceed " + succeedRows);
                } else {
                    System.out.println("Partial Succeed " + succeedRows);
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

            public void onFailed(BatchWriteRowRequest request, Exception e) {
                System.out.println("onFailed " + e.getMessage());
                e.printStackTrace();
            }
        };

        client.batchWriteRow(batchWriteRowRequest, callback);
    }
}
