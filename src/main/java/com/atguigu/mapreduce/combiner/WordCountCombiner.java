package com.atguigu.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: WordCountCombiner
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/25 17:20
 * @ Version: v1.0
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int num = 0;
        for (IntWritable value : values) {
            num += value.get();
        }
        outV.set(num);
        context.write(key, outV);
    }
}
