package com.atguigu.mapreduce.partitioner2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: FlowDriver
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/7 22:36
 * @ Version: v1.0
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1：获取job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2：设置jar包
        job.setJarByClass(FlowDriver.class);

        //3：关联mapper、reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4：设置mapper 输出的key value 类型；输出：手机号 text，FlowBean
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5：设置最终输出的key value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置分区和分区数量（reduceTasks）(reduceTask默认1cpu、1G内存)
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        //6；设置数据的输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\doc\\learning\\hadoop_learning\\test_code\\input\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\doc\\learning\\hadoop_learning\\test_code\\output5_partitioner"));

        //7：提交job
        boolean result = job.waitForCompletion(true);//Params:verbose – print the progress to the user
        System.exit(result ? 0 : 1);
    }
}
