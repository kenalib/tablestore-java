package com.example;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import junit.framework.TestCase;

public class TableStoreUtilTest extends TestCase {
    private String pkName;
    private String type;
    private String value;

    public void setUp() throws Exception {
        super.setUp();
        pkName = "pKey";
    }

    public void tearDown() {
    }

    public void testBuildPrimaryKey() {
        type = "string";
        value = "abc";
        PrimaryKey pKeyStr = TableStoreUtil.buildPrimaryKey(pkName, type, value);
        assertEquals("pKey:abc", pKeyStr.toString());

        type = "integer";
        value = "123";
        PrimaryKey pKeyInt = TableStoreUtil.buildPrimaryKey(pkName, type, value);
        assertEquals("pKey:123", pKeyInt.toString());
    }

    public void testGenColValue() {
        type = "string";
        value = "abc";
        ColumnValue columnStr = TableStoreUtil.genColValue(value, type);
        assertEquals("abc", columnStr.toString());

        type = "integer";
        value = "123";
        ColumnValue columnInt = TableStoreUtil.genColValue(value, type);
        assertEquals("123", columnInt.toString());
    }
}
