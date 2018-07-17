package com.example;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.AlwaysRetryStrategy;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;

import java.util.ResourceBundle;

class TableStoreClient {
    private SyncClient client;

    TableStoreClient() {
        client = createClient();
    }

    void shutdown() {
        client.shutdown();
    }

    private SyncClient createClient() {
        ResourceBundle res = ResourceBundle.getBundle("account");

        final String endPoint = res.getString("account.endPoint");
        final String accessKeyId = res.getString("account.accessKeyId");
        final String accessKeySecret = res.getString("account.accessKeySecret");
        final String instanceName = res.getString("account.instanceName");

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setConnectionTimeoutInMillisecond(5000);
        clientConfiguration.setSocketTimeoutInMillisecond(5000);
        clientConfiguration.setRetryStrategy(new AlwaysRetryStrategy());

        return new SyncClient(endPoint, accessKeyId, accessKeySecret, instanceName, clientConfiguration);
    }

    void runBatchWriteRowRequest(BatchWriteRowRequest batchWriteRowRequest) {
        System.out.println("Running Batch: " + batchWriteRowRequest.getRowsCount());
        BatchWriteRowResponse response = client.batchWriteRow(batchWriteRowRequest);

        if (response.isAllSucceed()) {
            System.out.println("All Succeed rows: " + response.getSucceedRows().size());
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
