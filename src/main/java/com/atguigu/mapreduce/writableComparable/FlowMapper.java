package com.atguigu.mapreduce.writableComparable;

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
public class FlowMapper extends Mapper<LongWritable, Text, /*Text, FlowBean*/ FlowBean, Text> {
    private FlowBean outK = new FlowBean();
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] split = line.split("\t");

        outV.set(split[0]);
        outK.setUpFlow(Long.parseLong(split[1]));
        outK.setDownFlow(Long.parseLong(split[2]));
        outK.setSumFlow();

        context.write(outK, outV);

    }

    //    /*private Text outK = new Text();
//    private FlowBean outV = new FlowBean();
//
//    @Override
//    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
//        //1：获取一行
//        //1 15267076088 192.168.100.1 www.atguigu.com 24681 299
//        //2	13846544121	192.196.100.2			264	0	200
//        String line = value.toString();
//
//        //2：切割
//        String[] split = line.split("\t");
//
//        //抓取想要的数据
//        //手机号、上下行流量
//        String phone = split[1];
//        String up =  split[split.length - 3];   //字符串操作tips：先获取长度，从后往前数第几个是想要的元素
//        String down = split[split.length - 2];
//
//        //封装
//        outK.set(phone);
//
//        outV.setUpFlow(Long.parseLong(up));
//        outV.setDownFlow(Long.parseLong(down));
//        outV.setSumFlow();
//
//        context.write(outK, outV);
//    }*/



}
