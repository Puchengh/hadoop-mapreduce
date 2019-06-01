package com.puchen.cn.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区规则
 * @author Administrator
 * key:map输出的key的类型
 * calue:map端输出的value的类型
 *
 */
public class MyPartition extends Partitioner<Text,IntWritable>{

	/**
	 * Text arg0, map输出的key的类型
	 * IntWritable arg1, map端输出的value的类型
	 * int arg2  job.numReduceTask()
	 */
	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		String k  =key.toString();
		char kc = k.charAt(0);
		if(kc>='a'&&kc<'j'){
			return 0;
		}else{
			return 1;
		}
	}
}
