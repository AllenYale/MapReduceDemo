package com.atguigu.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: WordCountDriver
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/5 21:43
 * @ Version: v1.0
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1 获取job
//        Configuration configuration = new Configuration();
//        Job job = Job.getInstance();

        //修改bug 把configuration作为参数传入 Job.getInstance(configuration)
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar包路径，通过全类名，反射获取jar包位置。
        job.setJarByClass(WordCountDriver.class);

        // 3 关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出的kV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setCombinerClass(WordCountCombiner.class);
//
//        job.setNumReduceTasks(0);
        job.setCombinerClass(WordCountReducer.class);

        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\doc\\learning\\hadoop_learning\\test_code\\input\\input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\doc\\learning\\hadoop_learning\\test_code\\output222"));

        // 7 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
