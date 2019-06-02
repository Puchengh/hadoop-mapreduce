package com.puchen.cn.DefineClass;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//对相同的手机号统计总上行流量  总下行流量  总流量
public class MyReduce extends Reducer<Text, FlowBean, Text, FlowBean>{

	
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//循环遍历求和
		
		int sumflow=0;
		int sundownflow=0;
		for (FlowBean fb:values) {
			sumflow+=fb.getUpoflow();
			sundownflow+=fb.getDownflow();
		}
		FlowBean fb1 = new FlowBean(sumflow,sundownflow);
		context.write(key, fb1);
	}
}
