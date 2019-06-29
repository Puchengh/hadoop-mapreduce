/**  
* Title: ReduceJoin.java  
* Description:   
* @author Puchen  
* @date 2019年6月25日 上午9:30:56  
* @version 1.0  
*/  
package com.puchen.cn.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.puchen.cn.join.ReduceJoin.MyMapper.MyReduce;

/**  
* <p>Title: ReduceJoin</p>  
* <p>Description: </p>  
* @author puchen  
* @date 2019年6月25日  上午9:30:56
*/
public class ReduceJoin {

	//key pid   value:剩下的打标记
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		String fileName = "";
		//context  上下文对象  
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			//获取文件名   
			//获取文件切片的相关信息   一个切片对饮一个maptask
			InputSplit inputSplit = context.getInputSplit();
			FileSplit fs = (FileSplit)inputSplit;
			//获取文件名
			String fileName = fs.getPath().getName();
		}
		/**
		 * 订单：order   商品信息:product
		 *由于mao中需要知道数据来源  所以最好在进入map函数之前可以获取文件的名字  这个事情给setup做
		 *map端做的事情：发送数据的时候需要打标记
		 *读取两个表中的数据  进行切分  发送
		 *key:公共字段  关联字段  pid
		 *value:剩下的  需要有标记  标记数据的来源
		 *reduce端
		 *接受过来，判断是来自于哪个表的数据进行拼接
		 */
		
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//解析出来每一行内容  打标记发送
			String[] infos = value.toString().split("\t");
			if(fileName.equals("order")) {
				//1001 20150710 P001 2
				k.set(infos[2]);
				//标记不要过程
				v.set("OR"+infos[0]+"\t"+infos[1]+"\t"+infos[3]);
				context.write(k, v);
			}else{
				//P001 小米5 c01 2000
				k.set(infos[0]);
				v.set("PR"+infos[1]+"\t"+infos[2]+"\t"+infos[3]);
				context.write(k, v);
			}
		}
		//1001 20150710 P001 2   小米5 c01 2000  输出格式
		static class MyReduce extends Reducer<Text, Text, Text, NullWritable>{
			//按照PID进行分组
			Text k = new Text();
			@Override
			protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
					throws IOException, InterruptedException {
				//在这个方法里面
				/**
				 * 关联关系  一对多的关系 一个商品对应多条订单
				 * 接受到的数据：一个表的数据只有一个  多个表的数据：可能有多个
				 * 将多的表拿过来和一的表中的数据分别进行拼接
				 */
				//两个数据需要封装到两个容器中
				List<String> orderList = new ArrayList<String>();
				List<String> proList = new ArrayList<String>();
				for (Text v : values) {
					String vv=v.toString();
					if(vv.startsWith("OR")) {
						orderList.add(vv.substring(2));
					}else {
						proList.add(vv.substring(2));
					}
				}
				
				//拼接的时候  什么时候才可以拼接  两个list都有的时候才拼接
				if(orderList.size()>0 && proList.size()>0) {
					//循环遍历多的  有拼接一的
					for(String ol:orderList){
						String res = key.toString()+ "\t" + ol+"\t"+proList.get(0);
						//最终结果写出
						k.set(res);
						context.write(k, NullWritable.get());
					}
				}
			}
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
		job.setJarByClass(ReduceJoin.class);
		
		//设置Mapper和Reduce的类
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		  
		//设置mapper的输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//设置reduce的输出类型
		//JDK的泛型是在1.5之后开始出现的  泛型是只在代码变异时候进行类型检查的
		//在代码运行的时候泛型会被自动擦除 所以在这里需要制定
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
//		job.setNumReduceTasks(2);
//		job.setPartitionerClass(MyPartition.class);
		
		FileInputFormat.addInputPath(job, new Path("/reduceJoin"));
		//输出路径：最终结果输出的路径  输出路径一定不能存在  hdfs怕吧原来的文件覆盖了  所以一定是一个全新的路径
		FileOutputFormat.setOutputPath(job, new Path("/reduceJoinOut"));
		
		job.waitForCompletion(true);
		
	}
	
}
