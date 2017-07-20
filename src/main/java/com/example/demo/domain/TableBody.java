package com.example.demo.domain;

/**
 * Created by juncheng on 17-7-20.
 */
public class TableBody {
    private String TableName;
    private String[] family;

    public String getTableName() {
        return TableName;
    }

    public void setTableName(String tableName) {
        TableName = tableName;
    }

    public String[] getFamily() {
        return family;
    }

    public void setFamily(String[] family) {
        this.family = family;
    }
}
