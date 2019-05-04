package com.xu.mr;

import com.xu.utils.ResourcesUtil;
import com.xu.kv.UnionDimension;
import com.xu.outputformat.MysqlOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class CalculateDurationDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //get configuration information and job
        Configuration configuration = HBaseConfiguration.create();
        Job job = Job.getInstance(configuration);
        //set driver jar
        job.setJarByClass(CalculateDurationDriver.class);
        //set mapper attributes
        TableMapReduceUtil.initTableMapperJob(ResourcesUtil.getProperties().getProperty("hbase.table.name"),
                new Scan(), CalculateDurationMapper.class, UnionDimension.class, Text.class, job);
        //set reducer
        job.setReducerClass(CalculateDurationReducer.class);
        //set OutPutFormat
        job.setOutputFormatClass(MysqlOutputFormat.class);
        //commit job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
