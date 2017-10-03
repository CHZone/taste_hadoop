package com.chzone.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author chzone MapReduce WordCount 作业主类
 */
public class WordCountApp {
    /**
     * @author chzone 读取输入文件
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        LongWritable one = new LongWritable(1);

        /*
         * key 为文件块对应文件的偏移量 可看着实现类WritableComparable接口的Long。 value 文件块内容 可看作实现了
         * Writable接口的String context map函数输出
         */
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            // 文件块内容转换为字符串
            String line = value.toString();
            // 拆分字符
            String[] words = line.split("\\s+");
            for (String word : words) {
                if(word.trim().length()>0){
                    context.write(new Text(word), one);
                }
            }
        }
    }

    public static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        /*
         * key 为 MyMapper.map 方法输出的context中的key values 为MyMapper.map输出的context
         * 中当前 key对应的one的集合返回的迭代器。 context 为reduce输出的结果
         */
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                Reducer<Text, LongWritable, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                // 统计
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        // 创建Configuration
        Configuration configuration = new Configuration();
        // 创建job
        Job job = Job.getInstance(configuration, "wordcount");
        // 设置job的处理类
        job.setJarByClass(WordCountApp.class);
        // 设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 设置作业的map相关参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置reduce相关参数
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
