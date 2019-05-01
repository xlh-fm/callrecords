package com.xu.kv;

import com.xu.kv.base.Dimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UnionDimension extends Dimension {
    private UserDimension userDimension = new UserDimension();
    private DateDimension dateDimension = new DateDimension();

    public UnionDimension() {
    }

    public UserDimension getUserDimension() {
        return userDimension;
    }

    public void setUserDimension(UserDimension userDimension) {
        this.userDimension = userDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    @Override
    public String toString() {
        return userDimension.toString() + dateDimension.toString();
    }

    @Override
    public int compareTo(Dimension o) {
        UnionDimension another = (UnionDimension) o;
        int ct = this.userDimension.compareTo(another.userDimension);
        if (ct == 0) {
            ct = this.dateDimension.compareTo(another.dateDimension);
        }
        return ct;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        userDimension.write(dataOutput);
        dateDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userDimension.readFields(dataInput);
        dateDimension.readFields(dataInput);
    }
}
