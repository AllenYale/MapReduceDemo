package com.atguigu.mapreduce.partitionerandWritableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * ClassName: ProvincePartitioner2
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/25 16:33
 * @ Version: v1.0
 */
public class ProvincePartitioner2 extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        //text 是手机号
        String phone = text.toString();

        String prePhone = phone.substring(0, 3);

        int partition;
        if ("136".equals(prePhone)) {
            partition = 0;
        } else if ("137".equals(prePhone)) {
            partition = 1;
        } else if ("138".equals(prePhone)) {
            partition = 2;
        } else if ("139".equals(prePhone)) {
            partition = 3;
        } else {
            partition = 4;
        }

        return partition;
    }
}
