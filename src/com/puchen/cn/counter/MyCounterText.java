package com.puchen.cn.counter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.puchen.cn.wordcount.MyDefineSort;


public class MyCounterText {

	//统计总记录条数    总字段数
	static class MyMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//获取计数器
			Counter lines_counter = context.getCounter(MyCounter.LINES);
			lines_counter.increment(1L);
			//获取下一个计数器  统计总的字段数
			Counter counts = context.getCounter(MyCounter.COUNT);
			String[] split = value.toString().split("\t");
			counts.increment(split.length);
		}
	}
	
	/**
	 * 如果没有设置reduce的个数 默认为1  如果不需要reduceTask一定要加上
	 * job.setNumReduceTasks(0);
	 * 结果文件格式  part-r-00000   reduce的输出结果
	 * 		   part-m-00000   map的输出结果
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//首先加载配置文件
				Configuration conf = new Configuration();
				//启动一个job  封装mapper和reduce  输入和输出
				Job job = Job.getInstance(conf);
				
				//设置的是计算程序的住驱动类  运行的时候达成jar包运行
				job.setJarByClass(MyDefineSort.class);
				
				//设置Mapper和Reduce的类
				job.setNumReduceTasks(0);
				job.setMapperClass(MyMapper.class);
				
				//设置mapper的输出类型
				job.setMapOutputKeyClass(NullWritable.class);
				job.setMapOutputValueClass(NullWritable.class);
				
				//设置reduce的输出类型
				//JDK的泛型是在1.5之后开始出现的  泛型是只在代码变异时候进行类型检查的
				//在代码运行的时候泛型会被自动擦除 所以在这里需要制定
//				job.setNumReduceTasks(3);
				
//				job.setCombinerClass(MyCombiner.class);
				
//				//设置切片的消息   单位是字节Byte
//				FileInputFormat.setMinInputSplitSize(job, 640);
//				FileInputFormat.setMaxInputSplitSize(job, 330);
				
				//设置输入路径和输出路径  运行的时候代码控制台输出的第一个参数
				//需要的统计的单词的路径
				FileInputFormat.addInputPath(job, new Path("/flowin01"));
				//输出路径：最终结果输出的路径  输出路径一定不能存在  hdfs怕吧原来的文件覆盖了  所以一定是一个全新的路径
				FileOutputFormat.setOutputPath(job, new Path("/flow_sort_01"));
				
				//job提交   还有一种不打印日志的  job.submit();
				job.waitForCompletion(true);
	}
}
