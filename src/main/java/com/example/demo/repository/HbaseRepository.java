package com.example.demo.repository;

import java.io.IOException;

//import com.example.demo.domain.Familys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import javax.annotation.Resources;


@Repository
public class HbaseRepository implements InitializingBean {

    // 声明静态配置
    private static Configuration config = null;

    static {
        config = HBaseConfiguration.create();
        config.set("hbase.zk.host", "localhost");
        config.set("hbase.zk.port", "2181");
    }
    /*@Resource(name = "hbaseConfiguration")
    private Configuration config;*/


    private HBaseAdmin admin;

    @Override
    public void afterPropertiesSet() throws IOException {
        admin = new HBaseAdmin(config);
    }

    /**
     * 创建初始化表
     *
     * @tableName 表名
     * @family 列族列表
     */
    public String initialize(String tableName, String[] CF_INFO) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (int i = 0; i < CF_INFO.length; i++) {
            desc.addFamily(new HColumnDescriptor(CF_INFO[i]));
        }
        if (admin.tableExists(tableName)) {
            System.out.println("table Exists!");
            if (!admin.isTableDisabled(tableName)) {
                System.out.println("Disabling " + tableName);
                admin.disableTable(tableName);
            }
            System.out.println("Deleting " + tableName);
            admin.deleteTable(tableName);
        }
        admin.createTable(desc);
        System.out.println("create table Success!");
        return "Init Table Success!";
    }

    /**
     * 创建表
     *
     * @tableName 表名
     * @family 列族列表
     */
    public String createTable(String tableName, String[] CF_INFO) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (int i = 0; i < CF_INFO.length; i++) {
            desc.addFamily(new HColumnDescriptor(CF_INFO[i]));
        }
        if (admin.tableExists(tableName)) {
            System.out.println("table Exists!");
            return "table Exists!";
//            if (!admin.isTableDisabled(tableName)) {
//                System.out.println("Disabling " + tableName);
//                admin.disableTable(tableName);
//            }
//            System.out.println("Deleting " + tableName);
//            admin.deleteTable(tableName);
        } else {
            admin.createTable(desc);
            System.out.println("create table Success!");
            return "Init Table Success!";
        }
    }

    /**
     * 删除表
     *
     * @tableName 表名
     */
    public String deleteTable(String tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            System.out.println("table Exists!");
            if (!admin.isTableDisabled(tableName)) {
                System.out.println("Disabling " + tableName);
                admin.disableTable(tableName);
            }
            System.out.println("Deleting " + tableName);
            admin.deleteTable(tableName);
            return tableName + "is deleted!";
        } else {
            return "Table not Exists!";
        }
    }

    /**
     * 删除表
     *
     * @tableName 表名
     */
    public void delete(String tableName) throws IOException {
//        HBaseAdmin admin = new HBaseAdmin(config);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(tableName + "is deleted!");
    }


    /**
     * 为表添加数据（适合知道有多少列族的固定表）
     *
     * @rowKey rowKey
     * @tableName 表名
     * @column1 第一个列族名
     * @value1 第一个列的值
     * @column2 第二个列族名
     * @value2 第二个列的值
     */
    public void addData(String tableName, String rowKey,
                        String[] column1, String[] value1,
                        String[] column2, String[] value2) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        HTable table = new HTable(config, Bytes.toBytes(tableName));// HTable负责跟记录相关的操作如增删改查等//
        // 获取表
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                .getColumnFamilies();

        for (HColumnDescriptor columnFamily : columnFamilies) {
            String familyName = columnFamily.getNameAsString(); // 获取列族名
            if (familyName.equals("article")) { // article列族put数据
                for (int j = 0; j < column1.length; j++) {
                    put.add(Bytes.toBytes(familyName),
                            Bytes.toBytes(column1[j]),
                            Bytes.toBytes(value1[j]));
                }
            }
            if (familyName.equals("author")) { // author列族put数据
                for (int j = 0; j < column2.length; j++) {
                    put.add(Bytes.toBytes(familyName),
                            Bytes.toBytes(column2[j]),
                            Bytes.toBytes(value2[j]));
                }
            }
        }
        table.put(put);
        System.out.println("add data Success!");
    }

    /**
     * 根据rwokey查询
     *
     * @rowKey rowKey
     * @tableName 表名
     */
    public Result getResult(String tableName, String rowKey)
            throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        HTable table = new HTable(config, Bytes.toBytes(tableName));// 获取表
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
        return result;
    }

    /**
     * 遍历查询hbase表
     *
     * @tableName 表名
     */
    public void getResultScann(String tableName) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        HTable table = new HTable(config, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:"
                            + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:"
                            + Bytes.toString(kv.getQualifier()));
                    System.out
                            .println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out
                            .println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }

    /**
     * 遍历查询hbase表
     *
     * @tableName 表名
     */
    public void getResultScann(String tableName, String start_rowkey,
                               String stop_rowkey) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner rs = null;
        HTable table = new HTable(config, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:"
                            + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:"
                            + Bytes.toString(kv.getQualifier()));
                    System.out
                            .println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out
                            .println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }

    /**
     * 查询表中的某一列
     *
     * @tableName 表名
     * @rowKey rowKey
     */
    public void getResultByColumn(String tableName, String rowKey,
                                  String familyName, String columnName) throws IOException {
        HTable table = new HTable(config, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
    }

    /**
     * 更新表中的某一列
     *
     * @tableName 表名
     * @rowKey rowKey 行
     * @familyName 列族名
     * @columnName 列名
     * @value 更新后的值
     */
    public void updateTable(String tableName, String rowKey,
                            String familyName, String columnName, String value)
            throws IOException {
        HTable table = new HTable(config, Bytes.toBytes(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
                Bytes.toBytes(value));
        table.put(put);
        System.out.println("update table Success!");
    }

    /**
     * 查询某列数据的多个版本
     *
     * @tableName 表名
     * @rowKey rowKey
     * @familyName 列族名
     * @columnName 列名
     */
    public void getResultByVersion(String tableName, String rowKey,
                                   String familyName, String columnName) throws IOException {
        HTable table = new HTable(config, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(5);
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
        /**
         * List<?> results = table.get(get).list(); Iterator<?> it =
         * results.iterator(); while (it.hasNext()) {
         * System.out.println(it.next().toString()); }
         */
    }

    /**
     * 删除指定的列
     *
     * @tableName 表名
     * @rowKey rowKey
     * @familyName 列族名
     * @columnName 列名
     */
    public void deleteColumn(String tableName, String rowKey,
                             String falilyName, String columnName) throws IOException {
        HTable table = new HTable(config, Bytes.toBytes(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.deleteColumns(Bytes.toBytes(falilyName),
                Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        System.out.println(falilyName + ":" + columnName + "is deleted!");
    }

    /**
     * 删除指定的行
     *
     * @tableName 表名
     * @rowKey rowKey
     */
    public void deleteAllColumn(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(config, Bytes.toBytes(tableName));
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteAll);
        System.out.println("all columns are deleted!");
    }
}
/*
    public void initialize() throws IOException {

        if (admin.tableExists(tableNameAsBytes)) {
            if (!admin.isTableDisabled(tableNameAsBytes)) {
                System.out.printf("Disabling %s\n", tableName);
                admin.disableTable(tableNameAsBytes);
            }
            System.out.printf("Deleting %s\n", tableName);
            admin.deleteTable(tableNameAsBytes);
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(CF_INFO);
        tableDescriptor.addFamily(columnDescriptor);

        admin.createTable(tableDescriptor);

    }

    public List<HbaseUser> findAll() {
        return hbaseTemplate.find(tableName, "cfInfo", new RowMapper<HbaseUser>() {
            @Override
            public HbaseUser mapRow(Result result, int rowNum) throws Exception {
                return new HbaseUser(Bytes.toString(result.getValue(CF_INFO, qUser)),
                        Bytes.toString(result.getValue(CF_INFO, qEmail)),
                        Bytes.toString(result.getValue(CF_INFO, qPassword)));
            }
        });

    }

    public HbaseUser save(final String userName, final String email, final String password) {
        return hbaseTemplate.execute(tableName, new TableCallback<HbaseUser>() {
            public HbaseUser doInTable(HTableInterface table) throws Throwable {
                HbaseUser user = new HbaseUser(userName, email, password);
                Put p = new Put(Bytes.toBytes(user.getName()));
                p.addColumn(CF_INFO, qUser, Bytes.toBytes(user.getName()));
                p.addColumn(CF_INFO, qEmail, Bytes.toBytes(user.getEmail()));
                p.addColumn(CF_INFO, qPassword, Bytes.toBytes(user.getPassword()));
                table.put(p);
                return user;
            }
        });
    }

    public void addUsers() {
        for (int i = 0; i < 10; i++) {
            userRepository.save("user" + i, "user" + i + "@yahoo.com", "password" + i);
        }
    }*/

