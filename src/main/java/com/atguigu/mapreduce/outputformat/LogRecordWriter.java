package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * ClassName: LogRecordWriter
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/25 23:20
 * @ Version: v1.0
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream atguiguOut;
    private FSDataOutputStream otherOut;

    public LogRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());
            atguiguOut = fileSystem.create(new Path("E:\\doc\\learning\\hadoop_learning\\test_code\\output\\atguigu.log"));
            otherOut = fileSystem.create(new Path("E:\\doc\\learning\\hadoop_learning\\test_code\\output\\other.log"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String keyString = key.toString();
        if (keyString.contains("atguigu")) {
            atguiguOut.writeBytes(keyString + "\n");
        } else {
            otherOut.writeBytes(keyString + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
