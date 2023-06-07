package com.atguigu.mapreduce.writable;

/**
 * ClassName: FlowBean
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/6 22:44
 * @ Version: v1.0
 */

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 具备系列化功能
 *
 */
public class FlowBean implements Writable {

    //上行流量、下行流量、总流量
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    //不会调用上方setSumFlow(long sumFlow) ,sumFlow为上行流量+上行流量之和
    public void setSumFlow(){
        this.sumFlow = upFlow + downFlow;
    }

    //空参构造
    public FlowBean() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //顺序和write保持一致
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
