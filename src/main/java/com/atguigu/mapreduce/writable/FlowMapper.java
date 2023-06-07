package com.atguigu.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: FlowMapper
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/6 23:00
 * @ Version: v1.0
 */
/*输入kv 索引，一行text
* 输出kv 手机号text，value输出 FlowBean
* */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //1：获取一行
        //1 15267076088 192.168.100.1 www.atguigu.com 24681 299
        String line = value.toString();

        //2：切割
        String[] split = line.split("\t");

        //抓取想要的数据
        //手机号、上下行流量
        String phone = split[1];

    }
}
