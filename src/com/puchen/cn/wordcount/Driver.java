package com.puchen.cn.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 驱动类  代码提交类
 * @author Administrator
 * 代码的运行有三种模式
 * 将代码达成jar包  提交到集群运行  真实生产中用
 * 缺点：不便于代码的调试
 * 
 * hadoop jar 包的名称  主类的全路径名  代码运行需要的参数    RunJar jarFile [mainClass] args...
 * hadoop jar /jarpackage/wordcount01.jar com.puchen.cn.wordcount.Driver /in /wordcount01
 *
 * _SECCESS:运行成功的标志文件  大小是0  只是一个标志
 * part-r-00000：最终结果文件  默认情况下是1个
 */
public class Driver {

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
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置reduce的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置输入路径和输出路径  运行的时候代码控制台输出的第一个参数
		//需要的统计的单词的路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//输出路径：最终结果输出的路径  输出路径一定不能存在  hdfs怕吧原来的文件覆盖了  所以一定是一个全新的路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//job提交   还有一种不打印日志的  job.submit();
		job.waitForCompletion(true);
		
		
	}
	
}
