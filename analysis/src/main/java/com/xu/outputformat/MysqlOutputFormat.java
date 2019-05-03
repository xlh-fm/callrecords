package com.xu.outputformat;

import com.xu.converter.DimensionConverter;
import com.xu.kv.CalculateDurationValue;
import com.xu.kv.UnionDimension;
import com.xu.utils.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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

    private static class MySQLRecordWriter extends RecordWriter<UnionDimension, CalculateDurationValue> {
        private Connection connection = null;
        private DimensionConverter dimensionConverter = null;
        private PreparedStatement preparedStatement = null;
        private int commitSize = 100;
        private int size = 0;
        private String sql = "INSERT INTO tb_records VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `call_sum` = ?," +
                "`call_duration_sum` = ?";

        public MySQLRecordWriter() {
            this.connection = JDBCUtil.getInstance();
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(sql);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            this.dimensionConverter = new DimensionConverter();
        }

        @Override
        public void write(UnionDimension key, CalculateDurationValue value) throws IOException, InterruptedException {
            //use key to get user dimension and date dimension id from mysql
            int userID = 0, dateID = 0;
            try {
                userID = dimensionConverter.getDimensionID(key.getUserDimension());
                dateID = dimensionConverter.getDimensionID(key.getDateDimension());
            } catch (SQLException e) {
                e.printStackTrace();
            }
            //get times and duration from value
            int times = value.getCount();
            int duration = value.getDuration();

            //create primary key in table
            String primaryKey = userID + "_" + dateID;

            //insert data into mysql
            try {
                preparedStatement.setString(1, primaryKey);
                preparedStatement.setInt(2, dateID);
                preparedStatement.setInt(3, userID);
                preparedStatement.setInt(4, times);
                preparedStatement.setInt(5, duration);
                preparedStatement.setInt(6, times);
                preparedStatement.setInt(7, duration);
                preparedStatement.addBatch();
                size++;

                if (size >= commitSize) {
                    preparedStatement.executeBatch();
                    connection.commit();
                    size = 0;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (preparedStatement != null) {
                try {
                    preparedStatement.executeBatch();
                    connection.commit();

                    JDBCUtil.close(connection, preparedStatement, null);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
