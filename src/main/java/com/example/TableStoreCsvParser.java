package com.example;

import com.alicloud.openservices.tablestore.model.RowPutChange;


interface TableStoreCsvParser {

    void parseHeaderRow(String[] cols);

    RowPutChange genRowPutChange(String[] cols);

}
