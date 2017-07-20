package com.example.demo.controller;

import com.example.demo.domain.TableBody;
import com.example.demo.repository.HbaseRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

/**
 * Created by juncheng on 17-7-19.
 */
@RestController
@RequestMapping("/hbase")
public class HbaseController {

    @Autowired
    private HbaseRepository hbaseRepository;

    @RequestMapping(value = "/test_init", method = RequestMethod.GET)
    public String Init() throws Exception {
        String tableName = "users";//表名
        String[] family = {"cfInfo", "aa"};//列族名

        String rz = hbaseRepository.initialize(tableName, family);
        System.out.println(rz);
        return rz;
    }

    @RequestMapping(value = "/delete/tableName/{tableName}", method = RequestMethod.GET)
    public String Delete(@PathVariable(name = "tableName") String tableName) throws Exception {
//        String tableName = "users";//表名

        String rz = hbaseRepository.deleteTable(tableName);
        System.out.println(rz);
        return rz;
    }

    @RequestMapping(value = "/create", method = RequestMethod.POST)
    public String create(@RequestBody TableBody table) throws Exception {
        String result = hbaseRepository.createTable(table.getTableName(), table.getFamily());
        return result;
    }

    public void test() throws IOException {
        String[] column1 = {"title", "content", "tag"};//列族
        String[] value1 = {
                "Head First HBase",
                "HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data.",
                "Hadoop,HBase,NoSQL"};
        String[] column2 = {"name", "nickname"};
        String[] value2 = {"nicholas", "lee"};
//        hbaseRepository.addData();
        hbaseRepository.addData("blog2", "rowkey2", column1, value1, column2, value2);
        hbaseRepository.addData("blog2", "rowkey3", column1, value1, column2, value2);

        // 遍历查询
        hbaseRepository.getResultScann("blog2", "rowkey4", "rowkey5");
        // 根据row key范围遍历查询
        hbaseRepository.getResultScann("blog2", "rowkey4", "rowkey5");

        // 查询
        hbaseRepository.getResult("blog2", "rowkey1");

        // 查询某一列的值
        hbaseRepository.getResultByColumn("blog2", "rowkey1", "author", "name");

        // 查询某一列的值
        hbaseRepository.getResultByColumn("blog2", "rowkey1", "author", "name");

        // 更新列
        hbaseRepository.updateTable("blog2", "rowkey1", "author", "name", "bin");

        // 查询某列的多版本
        hbaseRepository.getResultByVersion("blog2", "rowkey1", "author", "name");

        // 删除一列
        hbaseRepository.deleteColumn("blog2", "rowkey1", "author", "nickname");

        // 删除所有列
        hbaseRepository.deleteAllColumn("blog2", "rowkey1");

        // 删除表
        hbaseRepository.deleteTable("blog2");
    }
}
