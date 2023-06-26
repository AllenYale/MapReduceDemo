package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: LogReducer
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/25 23:13
 * @ Version: v1.0
 */
public class LogReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //相同Text会聚合成一条记录    text (null, null)
        //防止有相同数据、丢数据
        for (NullWritable value : values) {
            context.write(key, value);
        }
    }
}
