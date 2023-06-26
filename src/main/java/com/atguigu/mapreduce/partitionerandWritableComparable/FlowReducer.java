package com.atguigu.mapreduce.partitionerandWritableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: FlowReducer
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/7 22:26
 * @ Version: v1.0
 */
public class FlowReducer extends Reducer</*Text, FlowBean*/ FlowBean, Text, Text, FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //values 为手机号list，key为FlowBean（排序过）
        for (Text value : values) {
            context.write(value, key);
        }
    }
    //    private FlowBean outV = new FlowBean();
//
//    //经过map操作之后，Text key为手机号，values为相关手机号的不同Flowbean的集合
//    // reduce方法，相同的key才调用一次
//    @Override
//    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
//        // 1: 遍历集合累加值
//        long totalUp = 0;
//        long totalDown = 0;
//
//        for (FlowBean value : values) {
//            //遍历累加上行、下行流量
//            totalUp += value.getUpFlow();
//            totalDown += value.getDownFlow();
//        }
//
//        //2 封装：outK outV。
//        outV.setUpFlow(totalUp);
//        outV.setDownFlow(totalDown);
//        outV.setSumFlow();
//
//        //写出
//        context.write(key, outV);
//    }
}
