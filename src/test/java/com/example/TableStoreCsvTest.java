package com.example;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.Iterator;

public class TableStoreCsvTest extends TestCase {

    public void testIterator() throws IOException {
        String csvName = "csv/Crimes_-_2001_to_present_head.csv";

        String tableName = "ots_chicago_crime";
        int primaryColNum = 0;
        TableStoreCsvParser parser = new CrimeParser(tableName, primaryColNum);

        TableStoreCsv csv = new TableStoreCsv(csvName, parser);
        Iterator<RowPutChange> iterator = csv.iterator();

        if (iterator.hasNext()) {
            RowPutChange rowPutChange = iterator.next();

            ColumnValue colValue = TableStoreUtil.genColValue("HM664618", "string");
            assertTrue(rowPutChange.has("o_Case_Number", colValue));
        }
    }
}
