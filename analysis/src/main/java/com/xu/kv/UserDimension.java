package com.xu.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserDimension extends Dimension {
    private String phoneNo;
    private String userName;

    public UserDimension() {
    }

    public UserDimension(String phoneNo, String userName) {
        this.phoneNo = phoneNo;
        this.userName = userName;
    }

    public String getPhoneNo() {
        return phoneNo;
    }

    public void setPhoneNo(String phoneNo) {
        this.phoneNo = phoneNo;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public String toString() {
        return phoneNo + '\t' + userName;
    }

    @Override
    public int compareTo(Dimension o) {
        UserDimension another = (UserDimension) o;
        return this.phoneNo.compareTo(another.phoneNo);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.phoneNo);
        dataOutput.writeUTF(this.userName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phoneNo = dataInput.readUTF();
        this.userName = dataInput.readUTF();
    }
}
