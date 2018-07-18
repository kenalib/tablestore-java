package com.example;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;

import java.util.HashMap;
import java.util.Map;

class TableStoreAsyncClient extends TableStoreClient {
    private AsyncClient client;
    private Map<Integer, BatchWriteRowRequest> requested = new HashMap<Integer, BatchWriteRowRequest>();

    TableStoreAsyncClient() {
        readAccountInfo();
        createClientConfiguration();

        client = new AsyncClient(endPoint, accessKeyId, accessKeySecret, instanceName, clientConfiguration);
    }

    void shutdown() {
        client.shutdown();
    }

    int getCurrentRequestCount() {
        return requested.size();
    }

    void runBatchWriteRowRequest(final BatchWriteRowRequest batchWriteRowRequest) {
        System.out.println("Running Batch: " + batchWriteRowRequest.getRowsCount());
        requested.put(batchWriteRowRequest.hashCode(), batchWriteRowRequest);

        TableStoreCallback<BatchWriteRowRequest, BatchWriteRowResponse> callback = new TableStoreCallback<BatchWriteRowRequest, BatchWriteRowResponse>() {
            public void onCompleted(BatchWriteRowRequest request, BatchWriteRowResponse response) {
                requested.remove(request.hashCode());

                int succeedRows = response.getSucceedRows().size();
                if (!response.isAllSucceed()) {
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

                checkCurrentRequestsSize();
            }

            public void onFailed(BatchWriteRowRequest request, Exception e) {
                System.out.println("Resubmitting onFailed " + e.getMessage());
                client.batchWriteRow(request, this);

                checkCurrentRequestsSize();
            }

            private void checkCurrentRequestsSize() {
                int n = requested.size();
                String repeated = new String(new char[n]).replace("\0", "*");
                System.out.printf("remain: %2d %s\n", n, repeated);

                if (n == 0) {
                    client.shutdown();
                }
            }
        };

        client.batchWriteRow(batchWriteRowRequest, callback);
    }
}
