package com.chzone.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartitionerApp {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\\s+");
            context.write(new Text(words[0]), new LongWritable(Long.parseLong(words[1])));
        }
    }

    public static class MyPartitioner extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            if (key.toString().equals("xiaomi")) {
                return 0;
            }
            if (key.toString().equals("huawei")) {
                return 1;
            }
            if (key.toString().equals("iphone7")) {
                return 2;
            }
            return 3;
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
        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path(args[1]);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("out put file exists ,but has been deleted");
        }

        // Job
        Job job = Job.getInstance(configuration, "wordcount");
        // 写成类Partitioner导致报错
        // 找不到MyMapper
        job.setJarByClass(PartitionerApp.class);

        // FileInputFormat
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // Mapper
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Reducer
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Partitioner
        job.setPartitionerClass(MyPartitioner.class);
        // 设置4个reducer每个分区一个
        job.setNumReduceTasks(4);
        // FileOutputFormat
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
