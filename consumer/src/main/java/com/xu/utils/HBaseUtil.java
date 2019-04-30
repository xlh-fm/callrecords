package com.xu.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

/**
 * 1.create namespace
 * 2.determine if the table exists
 * 3.create table
 * 4.create splitkeys
 * 5.create rowkey
 */
public class HBaseUtil {
    public static Configuration configuration;

    static {
        configuration = HBaseConfiguration.create();
    }

    //1.create namespace
    public static void createNamespace(String nameSpace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //get namespaceDescriptor
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        //create namespace
        admin.createNamespace(namespaceDescriptor);

        admin.close();
        connection.close();
    }

    //2.determine if the table exists
    public static boolean tableExists(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        admin.close();
        connection.close();
        return exists;
    }

    //3.create table
    public static void createTable(String tableName, List<String> columnFamilys, int regions) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        if (tableExists(tableName)) {
            System.out.println("table " + tableName + " already exists");
            admin.close();
            connection.close();
            return;
        }
        //create tableDescriptor
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //add columnDescriptor
        for (String columnFamily : columnFamilys) {
            hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
        }
        //create table
        admin.createTable(hTableDescriptor, createSplitKeys(regions));
        admin.close();
        connection.close();
    }

    //4.create splitkeys
    //00| 01| 02| 03| 04| 05|
    private static byte[][] createSplitKeys(int regions) {
        byte[][] splitKeys = new byte[regions][];
        DecimalFormat format = new DecimalFormat("00");
        //loop create splitkeys
        for (int i = 0; i < regions; i++) {
            splitKeys[i] = Bytes.toBytes(format.format(i) + "|");
        }

        return splitKeys;
    }

    //5.create rowkey
    //0X_080XXXXXXXX_yyyy-MM-dd HH:mm:ss_timestamp_070XXXXXXXX_duration
    public static String getRowkey(String partition, String phone1, String setupTime, String timeStamp, String phone2,
                                   String duration) {
        return partition + "_" + phone1 + "_" + setupTime + "_" + timeStamp + "_" + phone2 + "_" + duration;
    }

    //get value of partition
    public static String getPartition(int regions, String phone, String callTime) {
        DecimalFormat format = new DecimalFormat("00");
        //get the last 8 digits of the phone number
        String subPhone = phone.substring(3, 11);
        //get year and month
        String ym = callTime.replace("-", "").substring(0, 6);
        //get partition
        int p = (Integer.valueOf(subPhone) ^ Integer.valueOf(ym)) % regions;
        return format.format(p);
    }

    public static void main(String[] args) {
        byte[][] splitKeys = createSplitKeys(6);
        for (byte[] splitKey : splitKeys) {
            System.out.println(Bytes.toString(splitKey));
        }
        String partition = getPartition(6, "08034578908", "2018-09-04 09:34:12");
        String rowkey = getRowkey(partition, "08034578908", "2018-09-04 09:34:12",
                "435345", "07074221099", "0235");
        System.out.println(rowkey);
    }
}