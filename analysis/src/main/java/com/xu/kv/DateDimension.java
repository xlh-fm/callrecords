package com.xu.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateDimension extends Dimension {
    private String year;
    private String month;
    private String day;

    public DateDimension() {
    }

    public DateDimension(String year, String month, String day) {
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    @Override
    public String toString() {
        return year + '\t' + month + '\t' + day;
    }

    @Override
    public int compareTo(Dimension o) {
        DateDimension another = (DateDimension) o;
        int ct = this.year.compareTo(another.year);
        if (ct == 0) {
            ct = this.month.compareTo(another.month);
            if (ct == 0) {
                ct = this.day.compareTo(another.day);
            }
        }
        return ct;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.year);
        dataOutput.writeUTF(this.month);
        dataOutput.writeUTF(this.day);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readUTF();
        this.month = dataInput.readUTF();
        this.day = dataInput.readUTF();
    }
}
