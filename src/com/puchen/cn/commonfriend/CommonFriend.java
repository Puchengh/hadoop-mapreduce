/**  
* Title: CommonFriend.java  
* Description:   
* @author Puchen  
* @date 2019年6月21日 上午11:21:06  
* @version 1.0  
*/  
package com.puchen.cn.commonfriend;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.puchen.cn.DefineClass.Driver;
import com.puchen.cn.DefineClass.MyMapper;
import com.puchen.cn.DefineClass.MyReduce;
import com.puchen.cn.wordcount.FlowBean;
import com.puchen.cn.wordcount.MyDefineSort;
/**  
* <p>Title: CommonFriend</p>  
* <p>Description: </p>    数据 类似   A:B,F,G,D 形式
* @author puchen  
* @date 2019年6月21日  上午11:21:06
*/
public class CommonFriend {
	//每一个maptask任务调用一次   就会找到这个类
	//B:A
	static class MyMapper_step01 extends Mapper<LongWritable, Text, Text, Text>{
		//map 一行调用一次   每一行创建一次对象
		Text ke = new Text();
		Text val = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			/**
			 * 读取到数据拆分右侧的好友  并且在左侧的用户意义拼接
			 */
			//两个元素  用户A    好友 B,F,G,D
			String[] user_friends = value.toString().split(":");
			String[] friends = user_friends[1].split(",");
			for(String friend : friends) {
				ke.set(friend);
				val.set(user_friends[0]);
				//每次调用函数数据都被写出  每行写出一次
				context.write(ke, val);
			}
			
		}
		
	}
	static class MyReduce_step01 extends Reducer<Text, Text, Text, Text>{
		//接受到的一组是有这个好友别的所有用户
		Text val = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuffer stringBuffer = new StringBuffer();
			for(Text val:values) {
				stringBuffer.append(val.toString()).append(",");
			}
			val.set(stringBuffer.substring(0,stringBuffer.length()-1));
			context.write(key, val);
		}
		
		
	}
	
	/**
	 * map端  拼接完了进行发送
	 * F-I:a
	 * F-I:b
	 * reduce端：整合
	 * F-I:a,b
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */	
	
	static class MyMapper_step02 extends Mapper<LongWritable, Text, Text, Text>{
		Text keyOut = new Text();
		Text valueOut = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] friend_user = value.toString().split("\t");
			String[] users = friend_user[1].split(",");
			
			for(String u1:users) {
				for(String u2:users) {
					if(u1.compareTo(u2)<0) {
						String k = u1+"-"+u2;
						keyOut.set(k);
						valueOut.set(friend_user[0]);
					}
				}
			}
			context.write(keyOut, valueOut);
		}
	}
	
	static class MyReduce_step02 extends Reducer<Text, Text, Text, Text>{
		Text val = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuffer str = new StringBuffer();
			for (Text text : values) {
				str.append(text).append(",");
				val.set(str.substring(0, str.length()-1));
			context.write(key, val);
		}
	}


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
				//首先加载配置文件
				Configuration conf = new Configuration();
				System.setProperty("HADOOP_USER_NAME","hadoop");
				conf.set("fs.defaultFS", "hdfs://bigdata-senior01.chybinmy.com:8020");
				//启动一个job  封装mapper和reduce  输入和输出
				Job job = Job.getInstance(conf);
				
				//设置的是计算程序的住驱动类  运行的时候达成jar包运行
				job.setJarByClass(CommonFriend.class);
				
				//设置Mapper和Reduce的类
				job.setMapperClass(MyMapper_step01.class);
				job.setReducerClass(MyReduce_step01.class);
				  
				//设置mapper的输出类型
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				
				//设置reduce的输出类型
				//JDK的泛型是在1.5之后开始出现的  泛型是只在代码变异时候进行类型检查的
				//在代码运行的时候泛型会被自动擦除 所以在这里需要制定
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				
//				job.setNumReduceTasks(2);
//				job.setPartitionerClass(MyPartition.class);
				
				FileInputFormat.addInputPath(job, new Path("/commonfriend"));
				//输出路径：最终结果输出的路径  输出路径一定不能存在  hdfs怕吧原来的文件覆盖了  所以一定是一个全新的路径
				FileOutputFormat.setOutputPath(job, new Path("/friend_step05"));
				
				/**
				 * job02的配置
				 */
				//启动一个job  封装mapper和reduce  输入和输出
				Job job2 = Job.getInstance(conf);
				
				//设置的是计算程序的住驱动类  运行的时候达成jar包运行
				job2.setJarByClass(CommonFriend.class);
				
				//设置Mapper和Reduce的类
				job2.setMapperClass(MyMapper_step02.class);
				job2.setReducerClass(MyReduce_step02.class);
				  
				//设置mapper的输出类型
				job2.setMapOutputKeyClass(Text.class);
				job2.setMapOutputValueClass(Text.class);
				
				//设置reduce的输出类型
				//JDK的泛型是在1.5之后开始出现的  泛型是只在代码变异时候进行类型检查的
				//在代码运行的时候泛型会被自动擦除 所以在这里需要制定
				
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				
//				job.setNumReduceTasks(2);
//				job.setPartitionerClass(MyPartition.class);
				
				FileInputFormat.addInputPath(job2, new Path("/friend_step05"));
				//输出路径：最终结果输出的路径  输出路径一定不能存在  hdfs怕吧原来的文件覆盖了  所以一定是一个全新的路径
				FileOutputFormat.setOutputPath(job2, new Path("/friend_out05"));
				
				//会将多个job当作一个运行组中的job提交  参数指的是组名随意
				JobControl jc = new JobControl("common_friend");
				ControlledJob ajob = new ControlledJob(job.getConfiguration());
				ControlledJob bjob = new ControlledJob(job2.getConfiguration());
				//添加依赖关系
				bjob.addDependingJob(ajob);
				
				jc.addJob(ajob);
				jc.addJob(bjob);
				
				Thread thread = new Thread(jc);
				thread.start();
				
				boolean allFinished = jc.allFinished();
				while(!allFinished) {
					thread.sleep(500);
				}
				thread.interrupt();
	}
	
	
	}
	
	

}
