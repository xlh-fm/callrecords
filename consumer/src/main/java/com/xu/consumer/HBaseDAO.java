package com.xu.consumer;

import com.xu.utils.HBaseUtil;
import com.xu.utils.ResourcesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 1.initialize the namespace
 * 2.create table
 * 3.store bulk data
 */
public class HBaseDAO {
    private Properties properties;
    private String nameSpace;
    private String tableName;
    private int regions;
    private List<String> columnFamilys;
    private SimpleDateFormat sdf;
    private Connection connection;
    private Table table;
    private List<Put> puts;
    //flag for calling and called
    private String flag;

    public HBaseDAO() throws IOException {
        properties = ResourcesUtil.getProperties();
        nameSpace = properties.getProperty("hbase.namespace");
        tableName = properties.getProperty("hbase.table.name");
        regions = Integer.valueOf(properties.getProperty("hbase.regions"));
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        connection = ConnectionFactory.createConnection(HBaseUtil.configuration);
        table = connection.getTable(TableName.valueOf(tableName));
        puts = new ArrayList<>();
        columnFamilys = new ArrayList<>();
        //"1" means calling number in front of rowkey
        flag = "1";
        for (String cf : properties.getProperty("hbase.columnFamilys").split(",")) {
            columnFamilys.add(cf);
        }
        //initialize the namespace
        try {
            HBaseUtil.createNamespace(nameSpace);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //create table
        HBaseUtil.createTable(tableName, columnFamilys, regions);
    }

    //store bulk data
    public void putData(String value) throws ParseException, IOException {
        if (value == null) {
            return;
        }
        String[] splits = value.split(",");
        String phone1 = splits[0];
        String phone2 = splits[1];
        String setupTime = splits[2];
        String duration = splits[3];
        long setupTimeStamp = sdf.parse(setupTime).getTime();

        //get value of partition
        String partition = HBaseUtil.getPartition(regions, phone1, setupTime);
        //create rowkey
        String rowKey = HBaseUtil.getRowkey(partition, phone1, setupTime, setupTimeStamp + "", phone2, flag, duration);
        //create put
        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(columnFamilys.get(0)), Bytes.toBytes("phone1"), Bytes.toBytes(phone1));
        put.addColumn(Bytes.toBytes(columnFamilys.get(0)), Bytes.toBytes("setupTime"), Bytes.toBytes(setupTime));
        put.addColumn(Bytes.toBytes(columnFamilys.get(0)), Bytes.toBytes("setupTimeStamp"), Bytes.toBytes
                (setupTimeStamp +
                ""));
        put.addColumn(Bytes.toBytes(columnFamilys.get(0)), Bytes.toBytes("phone2"), Bytes.toBytes(phone2));
        put.addColumn(Bytes.toBytes(columnFamilys.get(0)), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        put.addColumn(Bytes.toBytes(columnFamilys.get(0)), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        puts.add(put);
        if (puts.size() >= 100) {
            table.put(puts);
            puts.clear();
        }
    }

    //put the remaining data
    public void close() throws IOException {
        table.put(puts);
        table.close();
        connection.close();
    }
}