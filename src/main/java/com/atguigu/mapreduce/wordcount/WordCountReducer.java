package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: WordCountReducer
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/5 21:43
 * @ Version: v1.0
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        int sum = 0;

        //atguigu, (1,1)
        for (IntWritable value : values) {
            sum += value.get();
        }

        outV.set(sum);

        //写出
        context.write(key, outV);
    }
}
