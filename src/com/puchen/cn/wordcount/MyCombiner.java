package com.puchen.cn.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

	//重写reduce方法  这个reduce方法对应的是一个切片（mapTask）的数据进行统计
	@Override
	protected void reduce(Text key, Iterable<IntWritable> vaules,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0;
		for(IntWritable v : vaules){
			sum+=v.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
