package com.xu.mr;

import com.xu.kv.CalculateDurationValue;
import com.xu.kv.UnionDimension;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CalculateDurationReducer extends Reducer<UnionDimension, Text, UnionDimension, CalculateDurationValue> {
    private int count;
    private int duration;
    private CalculateDurationValue v = new CalculateDurationValue();

    @Override
    protected void reduce(UnionDimension key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        count = 0;
        duration = 0;

        for (Text value : values) {
            count++;
            duration += Integer.valueOf(value.toString());
        }
        v.setCount(count);
        v.setDuration(duration);

        context.write(key, v);
    }
}
