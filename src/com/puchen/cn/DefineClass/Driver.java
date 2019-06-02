package com.puchen.cn.DefineClass;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//首先加载配置文件
		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME","hadoop");
		//启动一个job  封装mapper和reduce  输入和输出
		Job job = Job.getInstance(conf);
		
		//设置的是计算程序的住驱动类  运行的时候达成jar包运行
		job.setJarByClass(Driver.class);
		
		//设置Mapper和Reduce的类
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		  
		//设置mapper的输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//设置reduce的输出类型
		//JDK的泛型是在1.5之后开始出现的  泛型是只在代码变异时候进行类型检查的
		//在代码运行的时候泛型会被自动擦除 所以在这里需要制定
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
//		job.setNumReduceTasks(2);
//		job.setPartitionerClass(MyPartition.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/flowin01"));
		//输出路径：最终结果输出的路径  输出路径一定不能存在  hdfs怕吧原来的文件覆盖了  所以一定是一个全新的路径
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/flowout01"));
		
		//job提交   还有一种不打印日志的  job.submit();
		job.waitForCompletion(true);
	}
}
