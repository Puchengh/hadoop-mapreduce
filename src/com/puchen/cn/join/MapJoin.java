/**  
* Title: ReduceJoin.java  
* Description:   
* @author Puchen  
* @date 2019年6月25日 上午9:30:56  
* @version 1.0  
*/  
package com.puchen.cn.join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**  
* <p>Title: ReduceJoin</p>  
* <p>Description: </p>  
* @author puchen  
* @date 2019年6月25日  上午9:30:56
*/
public class MapJoin {

	//map端join  key：PID 
	static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		/**
		 * 去读缓存中的数据  封装到容器中
		 * 读：流
		 * 容器：因为等会要进行匹配 匹配的时候 pid匹配  最好pid先抽出来
		 */
		Map<String,String> map=new HashMap<String,String>();
		Text k = new Text();
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			//获取缓存文件  context.getLocalCacheFiles();
			Path path = context.getLocalCacheFiles()[0];
			String p=path.toString();
			BufferedReader br = new BufferedReader(new FileReader(p));
			String line=null;
			while((line=br.readLine())!=null) {
				//P0002   锤子T1 c01 3500
				String[] infos = line.split("\t");
				map.put(infos[0], infos[1]+"\t"+infos[2]+"\t"+infos[3]);
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//1001 20150710 P0001 3
			String[] infos = value.toString().split("\t");
			//进行关联pid到马匹中匹配  如果包含  证明匹配上了
			String pid = infos[2];
			if(map.containsKey(pid)) {
				String res=value.toString()+map.get(pid);
				k.set(res);
				context.write(k, NullWritable.get());
			}
		}
	
		
	}
	static class MyReduce extends Reducer<Text, Text, Text, NullWritable>{
			
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		//首先加载配置文件
		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME","hadoop");
		conf.set("fs.defaultFS", "hdfs://bigdata-senior01.chybinmy.com:8020");
		Job job = Job.getInstance(conf);
		
		//设置的是计算程序的住驱动类  运行的时候达成jar包运行
		job.setJarByClass(MapJoin.class);
		
		//设置Mapper和Reduce的类
		job.setMapperClass(MyMapper.class);
		  
		//这里的输出指的是最终的输出结果	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//没有没有reducetask一定要加上把reduce设置为0
		job.setNumReduceTasks(0);
		
		job.addCacheArchive(new URI("/info/product"));
		
//		job.setNumReduceTasks(2);
//		job.setPartitionerClass(MyPartition.class);
		
		FileInputFormat.addInputPath(job, new Path("/info/order"));
		//输出路径：最终结果输出的路径  输出路径一定不能存在  hdfs怕吧原来的文件覆盖了  所以一定是一个全新的路径
		FileOutputFormat.setOutputPath(job, new Path("/reduceJoinOut"));
		
		job.waitForCompletion(true);
		
	}
	
}
