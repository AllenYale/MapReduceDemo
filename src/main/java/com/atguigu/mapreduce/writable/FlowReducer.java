package com.atguigu.mapreduce.writable;

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
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean outV = new FlowBean();

    //reduce方法，相同的key才调用一次
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        // 1: 遍历集合累加值
        long totalUp = 0;
        long totalDown = 0;

        for (FlowBean value : values) {
            totalUp = value.getUpFlow();
            totalDown = value.getDownFlow();
        }

        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        //写出
        context.write(key, outV);
    }
}
