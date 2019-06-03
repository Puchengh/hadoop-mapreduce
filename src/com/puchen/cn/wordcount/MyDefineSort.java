package com.puchen.cn.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDefineSort {

	
	static class MyMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, FlowBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//将每一行的数据取出封装到flowBean中
			String[] split = value.toString().split("\t");
			FlowBean flowBean = new FlowBean(split[0], Integer.parseInt(split[1].trim()), 
					 Integer.parseInt(split[2].trim()),  Integer.parseInt(split[3].trim()));
			context.write(flowBean, NullWritable.get());
		}
	}
	
	static class MyReduce extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable>{
		/**
		 * 分组    
		 * key为自定义类型   如果数据丢失  则证明不是按照地址分组的
		 * 按照我们自定义的compareTo()方法进行分组的
		 * 没有自定分组的时候如果使用的自带的类型  怎调用的是自带哦类型的compareTo()
		 * 根据这个方法判断是否为一组
		 * 自定义的类呢？更具自定义类中的comparaTO方法   比较set《自定义》
		 */
		@Override
		protected void reduce(FlowBean key, Iterable<NullWritable> values,
				Reducer<FlowBean, NullWritable, FlowBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (NullWritable nullWritable : values) {
				context.write(key, NullWritable.get());
			}
			
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//首先加载配置文件
				Configuration conf = new Configuration();
				//启动一个job  封装mapper和reduce  输入和输出
				Job job = Job.getInstance(conf);
				
				//设置的是计算程序的住驱动类  运行的时候达成jar包运行
				job.setJarByClass(MyDefineSort.class);
				
				//设置Mapper和Reduce的类
				job.setMapperClass(MyMapper.class);
				job.setReducerClass(MyReduce.class);
				
				//设置mapper的输出类型
				job.setMapOutputKeyClass(FlowBean.class);
				job.setMapOutputValueClass(NullWritable.class);
				
				//设置reduce的输出类型
				//JDK的泛型是在1.5之后开始出现的  泛型是只在代码变异时候进行类型检查的
				//在代码运行的时候泛型会被自动擦除 所以在这里需要制定
				
				job.setOutputKeyClass(FlowBean.class);
				job.setOutputValueClass(NullWritable.class);
				
//				job.setNumReduceTasks(3);
				
//				job.setCombinerClass(MyCombiner.class);
				
//				//设置切片的消息   单位是字节Byte
//				FileInputFormat.setMinInputSplitSize(job, 640);
//				FileInputFormat.setMaxInputSplitSize(job, 330);
				
				//设置输入路径和输出路径  运行的时候代码控制台输出的第一个参数
				//需要的统计的单词的路径
				FileInputFormat.addInputPath(job, new Path("/flowout01"));
				//输出路径：最终结果输出的路径  输出路径一定不能存在  hdfs怕吧原来的文件覆盖了  所以一定是一个全新的路径
				FileOutputFormat.setOutputPath(job, new Path("/flow_sort_01"));
				
				//job提交   还有一种不打印日志的  job.submit();
				job.waitForCompletion(true);
	}
}
