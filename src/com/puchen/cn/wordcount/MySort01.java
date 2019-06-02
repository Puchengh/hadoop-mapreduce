package com.puchen.cn.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 按照出现的次数排序
 * @author Administrator
 *
 */
public class MySort01 {

	static class MyMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			//默认情况下  输出结果  key和value之间的分隔符是制表符
			// TODO Auto-generated method stub
			String[] datas = value.toString().split("\t");
			String word=datas[0];
			int count=Integer.parseInt(datas[1]);
			context.write(new IntWritable(count), new Text(word));
		}
		
	}
	
	static class MyReduce extends Reducer<IntWritable, Text, Text, IntWritable>{
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			for(Text v:values){
				context.write(v, key);
				
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//首先加载配置文件
		Configuration conf = new Configuration();
		//启动一个job  封装mapper和reduce  输入和输出
		Job job = Job.getInstance(conf);
		
		//设置的是计算程序的住驱动类  运行的时候达成jar包运行
		job.setJarByClass(Driver.class);
		
		//设置Mapper和Reduce的类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);
		
		//设置mapper的输出类型
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//设置reduce的输出类型
		//JDK的泛型是在1.5之后开始出现的  泛型是只在代码变异时候进行类型检查的
		//在代码运行的时候泛型会被自动擦除 所以在这里需要制定
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
//		job.setNumReduceTasks(3);
		
//		job.setCombinerClass(MyCombiner.class);
		
//		//设置切片的消息   单位是字节Byte
//		FileInputFormat.setMinInputSplitSize(job, 640);
//		FileInputFormat.setMaxInputSplitSize(job, 330);
		
		//设置输入路径和输出路径  运行的时候代码控制台输出的第一个参数
		//需要的统计的单词的路径
		FileInputFormat.addInputPath(job, new Path("wc_combiner_02"));
		//输出路径：最终结果输出的路径  输出路径一定不能存在  hdfs怕吧原来的文件覆盖了  所以一定是一个全新的路径
		FileOutputFormat.setOutputPath(job, new Path("/sort01"));
		
		//job提交   还有一种不打印日志的  job.submit();
		job.waitForCompletion(true);
		
	}
	
}
