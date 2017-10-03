package com.chzone.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author chzone 执行前删除目录
 */
public class WordCountApp2 {
    /**
     * 为毛要搞成静态呢
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            // 接受数据
            String str = value.toString();
            String[] words = str.split("\\s+");
            for (String word : words) {
                context.write(new Text(word), one);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                Reducer<Text, LongWritable, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        // 清除已经存在的输出目录
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("out put file exists, but is has been deleted");
        }

        // 创建Job
        Job job = Job.getInstance();
        // 设置作业类
        job.setJarByClass(WordCountApp2.class);

        // 设置作业处理文件路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 设置map相关参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置reduce相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置作业输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println(job.waitForCompletion(true) ? 0 : 1);

    }

}
