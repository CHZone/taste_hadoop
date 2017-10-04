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

public class CombinerApp {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        LongWritable one = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            String s = value.toString();
            String[] strarr = s.split("\\s+");
            for (String string : strarr) {
                context.write(new Text(string), one);
            }
        }
    }
    
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values ,
                Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
    
    public static void main(String[] args) throws Exception{
        // 获取 Configuration和FileSystem
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(new Path(args[1]))){
            fileSystem.delete(new Path(args[1]),true);
            System.out.println("delete existing output files");
        }
        // 获取作业实例
        Job job = Job.getInstance();
        //设置作业主类
        job.setJarByClass(CombinerApp.class);
        
        // 设置job输入FileInputFormat
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        
        // 设置 mapper
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        //  设置Reducer
        
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        //设置Commbiner
        job.setCombinerClass(MyReducer.class);
        
        // 设置job的FileOutputFormat
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 退出
        System.exit(job.waitForCompletion(true)?0:1);;
        
    }
}
