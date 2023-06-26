package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: LogMapper
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/25 23:09
 * @ Version: v1.0
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //输入 索引+text
        //http://www.baidu.com
        //http://www.google.com
        //http://cn.bing.com
        //输出 text+nullwritable
        context.write(value, NullWritable.get());
    }
}
