package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: WordCountMapper
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/5 21:42
 * @ Version: v1.0
 */

/**
 * KEYIN, map阶段输入key的类型：LongWritable（偏移量）
 * VALUEIN, map阶段输入value类型：Text
 * KEYOUT, map阶段输出key类型：Text
 * VALUEOUT，map阶段输出value类型：IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outK = new Text();

    //在map阶段，不进行聚合。只是将每个单词切割出来，（atguigu，1）
    private IntWritable outV = new IntWritable(1);

    /**
     * map方法一行调用一次
     *
     * @param key     map输入的key和value
     * @param value
     * @param context 对应上下文、负责map reduce 系统之间联络，充当联络员角色
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //获取一行
        String string = value.toString();

        //切割一行所有单词
        String[] split = string.split(" ");

        //循环写出，一行有n个单词，循环n次
        for (String s : split) {
            outK.set(s);

            //写出
            context.write(outK, outV);
        }


    }
}
