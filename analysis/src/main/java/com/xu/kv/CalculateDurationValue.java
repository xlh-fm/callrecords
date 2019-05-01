package com.xu.kv;

import com.xu.kv.base.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CalculateDurationValue extends Value {
    private int count;
    private int duration;

    public CalculateDurationValue() {
    }

    public CalculateDurationValue(int count, int duration) {
        this.count = count;
        this.duration = duration;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        return count + "\t" + duration;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeInt(duration);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        duration = dataInput.readInt();
    }
}
