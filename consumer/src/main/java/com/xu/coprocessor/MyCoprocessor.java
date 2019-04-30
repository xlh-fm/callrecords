package com.xu.coprocessor;

import com.xu.utils.HBaseUtil;
import com.xu.utils.ResourcesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MyCoprocessor extends BaseRegionObserver {
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability
            durability) throws IOException {
        //determine if it is the required table
        String tableName1 = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        String tableName2 = ResourcesUtil.getProperties().getProperty("hbase.table.name");
        if (!tableName1.equals(tableName2)) {
            return;
        }

        String oldRowKey = Bytes.toString(put.getRow());
        String[] splits = oldRowKey.split("_");
        //if flag is 2, there is no need to use coprocessor
        if (splits[5].equals("2")) {
            return;
        }
        //get column family
        List<String> columnFamilys = new ArrayList<>();
        for (String cf : ResourcesUtil.getProperties().getProperty("hbase.columnFamilys").split(",")) {
            columnFamilys.add(cf);
        }
        //get data from rowkey
        String callingPhone = splits[1];
        String setupTime = splits[2];
        String setupTimeStamp = splits[3];
        String calledPhone = splits[4];
        String duration = splits[6];
        int regions = Integer.valueOf(ResourcesUtil.getProperties().getProperty("hbase.regions"));
        //get value of new partition
        String partition = HBaseUtil.getPartition(regions, calledPhone, setupTime);
        String newRowkey = HBaseUtil.getRowkey(partition, calledPhone, setupTime, setupTimeStamp, callingPhone, "2",
                duration);
        Put newPut = new Put(Bytes.toBytes(newRowkey));

        newPut.addColumn(Bytes.toBytes(columnFamilys.get(1)), Bytes.toBytes("phone1"), Bytes.toBytes
                (calledPhone));
        newPut.addColumn(Bytes.toBytes(columnFamilys.get(1)), Bytes.toBytes("setupTime"), Bytes.toBytes
                (setupTime));
        newPut.addColumn(Bytes.toBytes(columnFamilys.get(1)), Bytes.toBytes("setupTimeStamp"), Bytes.toBytes
                (setupTimeStamp +
                        ""));
        newPut.addColumn(Bytes.toBytes(columnFamilys.get(1)), Bytes.toBytes("phone2"), Bytes.toBytes
                (callingPhone));
        newPut.addColumn(Bytes.toBytes(columnFamilys.get(1)), Bytes.toBytes("flag"), Bytes.toBytes("2"));
        newPut.addColumn(Bytes.toBytes(columnFamilys.get(1)), Bytes.toBytes("duration"), Bytes.toBytes
                (duration));

        Connection connection = ConnectionFactory.createConnection(HBaseUtil.configuration);
        Table table = connection.getTable(TableName.valueOf(tableName1));
        table.put(newPut);

        table.close();
        connection.close();
    }
}
