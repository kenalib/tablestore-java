package com.example;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;

class TableStoreUtil {
    static PrimaryKey buildPrimaryKey(String pkName, String pkType, String pkValue) {
        if (pkType.equals("string")) {
            return buildPrimaryKey(pkName, pkValue);
        } else if (pkType.equals("integer")) {
            int tmp = Integer.parseInt(pkValue);
            return buildPrimaryKey(pkName, tmp);
        } else {
            throw new RuntimeException("Type not found");
        }
    }

    private static PrimaryKey buildPrimaryKey(String pkName, long pkValue) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pkName, PrimaryKeyValue.fromLong(pkValue));
        return primaryKeyBuilder.build();
    }

    private static PrimaryKey buildPrimaryKey(String pkName, String pkValue) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pkName, PrimaryKeyValue.fromString(pkValue));
        return primaryKeyBuilder.build();
    }

    static ColumnValue genColValue(String col, String colType) {
        ColumnValue colValue;

        if (colType.equals("string")) {
            colValue = ColumnValue.fromString(col);
        } else if (colType.equals("integer")) {
            int tmp = Integer.parseInt(col);
            colValue = ColumnValue.fromLong(tmp);
        } else if (colType.equals("boolean")) {
            boolean tmp = Boolean.parseBoolean(col);
            colValue = ColumnValue.fromBoolean(tmp);
        } else if (colType.equals("double")) {
            double tmp = Double.parseDouble(col);
            colValue = ColumnValue.fromDouble(tmp);
        } else {
            throw new RuntimeException("type not found");
        }

        return colValue;
    }
}
