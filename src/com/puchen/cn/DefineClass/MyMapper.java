package com.puchen.cn.DefineClass;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//map端最终输出的key:手机号  Text  value:floeBean
public class MyMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split("\t");
		String phoneNum = split[1];
		FlowBean fb  = new FlowBean(Integer.parseInt(split[split.length-2]),Integer.parseInt(split[split.length-2]));
		context.write(new Text(phoneNum), fb);
	}

}
