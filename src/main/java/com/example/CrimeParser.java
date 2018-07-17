package com.example;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowPutChange;

import java.util.ArrayList;
import java.util.ResourceBundle;

class CrimeParser implements TableStoreCsvParser {
    private String tableName;
    private int primaryColNum;
    private ResourceBundle type;
    private ArrayList<String> headers = new ArrayList<String>();
    private String primaryName;
    private String primaryType;

    public CrimeParser(String tableName, int primaryColNum) {
        this.tableName = tableName;
        this.primaryColNum = primaryColNum;
        type = ResourceBundle.getBundle(tableName);
    }

    public void parseHeaderRow(String[] cols) {
        for (String col : cols) {
            col = col.replace(" ", "_");
            headers.add("o_" + col);
        }

        primaryName = headers.get(primaryColNum);
        primaryType = type.getString(primaryName);
    }

    public RowPutChange genRowPutChange(String[] cols) {
        PrimaryKey primaryKey = TableStoreUtil.buildPrimaryKey(primaryName, primaryType, cols[primaryColNum]);
        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);
        int j = 0;

        for (String col : cols) {
            if (j == 0 || col.equals("")) {
                j++;
                continue;
            }

            col = removeDoubleQuotes(col);
            String colName = headers.get(j);
            String colType = type.getString(colName);

            try {
                ColumnValue colValue = TableStoreUtil.genColValue(col, colType);
                rowPutChange.addColumn(new Column(colName, colValue));
            } catch (NumberFormatException e) {
                System.out.println("Error: " + primaryKey.toString() + " " + colName);
                throw e;
            }

            j++;
        }

        if (j != headers.size()) {
            throw new RuntimeException("Columns size differ from headers");
        }

        return rowPutChange;
    }

    private String removeDoubleQuotes(String input) {
        if (input.startsWith("\"") && input.endsWith("\"")) {
            input = input.subSequence(1, input.length() - 1).toString();
        }
        return input;
    }
}
