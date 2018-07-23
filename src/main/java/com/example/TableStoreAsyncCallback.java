package com.example;

import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;

class TableStoreAsyncCallback implements TableStoreCallback<BatchWriteRowRequest, BatchWriteRowResponse> {
    private TableStoreAsyncClient asyncClient;

    void setAsyncClient(TableStoreAsyncClient asyncClient) {
        this.asyncClient = asyncClient;
    }

    public void onCompleted(BatchWriteRowRequest req, BatchWriteRowResponse res) {
        asyncClient.removeRequested(req.hashCode());

        if (!res.isAllSucceed()) {
            System.out.println("Partial Succeed " + res.getSucceedRows().size());

            for (BatchWriteRowResponse.RowResult row : res.getFailedRows()) {
                System.out.println("Failed rows:" + req.getRowChange(row.getTableName(), row.getIndex()).getPrimaryKey());
                System.out.println("Cause of failure:" + row.getError());
            }

            BatchWriteRowRequest retryRequest = req.createRequestForRetry(res.getFailedRows());
            System.out.println("retryRequest count: " + retryRequest.getRowsCount());
            // client.batchWriteRow(retryRequest, this);
            asyncClient.batchWriteRow(retryRequest);
        }

        checkCurrentRequestsSize();
    }

    public void onFailed(BatchWriteRowRequest req, Exception e) {
        System.out.println("Resubmitting onFailed " + e.getMessage());
        asyncClient.batchWriteRow(req);

        checkCurrentRequestsSize();
    }

    private void checkCurrentRequestsSize() {
        int n = asyncClient.getCurrentRequestCount();
        String repeated = new String(new char[n]).replace("\0", "*");
        System.out.printf("remain: %2d %s\n", n, repeated);

        if (n == 0) {
            asyncClient.shutdown();
        }
    }
}
