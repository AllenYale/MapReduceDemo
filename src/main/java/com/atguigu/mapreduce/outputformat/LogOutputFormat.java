package com.atguigu.mapreduce.outputformat;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: LogOutputFormat
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/25 23:18
 * @ Version: v1.0
 */
public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        LogRecordWriter lrw = new LogRecordWriter(job);


        return lrw;
    }
}
