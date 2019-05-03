package com.xu.outputformat;

import com.xu.kv.CalculateDurationValue;
import com.xu.kv.UnionDimension;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class MysqlOutputFormat extends OutputFormat<UnionDimension, CalculateDurationValue> {
    private FileOutputCommitter committer = null;

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException,
            InterruptedException {
        return new MySQLRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException,
            InterruptedException {
        if (this.committer == null) {
            Path output = getOutputPath(context);
            this.committer = new FileOutputCommitter(output, context);
        }

        return this.committer;
    }

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
        return name == null ? null : new Path(name);
    }

    private static class MySQLRecordWriter<UnionDimension, CalculateDurationValue> extends
            RecordWriter<UnionDimension, CalculateDurationValue> {
        private Connection connection = null;
        private PreparedStatement preparedStatement = null;

        @Override
        public void write(UnionDimension key, CalculateDurationValue value) throws IOException, InterruptedException {
            //use key to get user dimension and date dimension from mysql

            //get times and duration from value

            //insert data into mysql
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        }
    }
}
