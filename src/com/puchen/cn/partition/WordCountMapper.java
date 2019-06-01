package com.puchen.cn.partition;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper类中的四个范型
 * 输入就是需要统计的文件  这个文件应该是以一行一次
 * KEYIN ：输入的Key的泛型  每一行的偏移量  maperreduce底层文件输入依赖流的方式   字节流  \r\n  一般情况下没用 Long
 * VALUEIN：输入的值的类型  值的是一行的内容
 * KEYOUT：输出的key的类型   就是单词
 * VALUEOUT： 输出的value的类型   标记1  便于统计
 * @author Administrator
 * 当数据需要持久化到磁盘或者进行网络传输的时候必须要进行序列化和饭序列化
 * 序列化：原始数据 ----二进制
 * 反序列化：二进制----原始数据、
 * mapreduce处理数据的时候不然要经过持久化磁盘或者网络传输   String-->serializable  连同类结构进行序列化和反序列化
 * hadoop提供了一套序列化和反序列结构  Writable  有点：轻便   不会序列化类结构
 * 对应的8中基本数据类型和string都实现好了接口
 * int--intwritable
 * String ---Text
 * java中的数据类型 转化为hadoop中对应的数据  new该类型对象（java中的值）
 * hadoop中的类型转化为java中的类型 get()
 * 
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * 调用频率：一行一次
	 * LongWritable key:每一行的起始偏移量 
	 * Text value：每一行的内容
	 * Context contex：上下文对象
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 获取到每一行的内容 进行单词的切分  将每个单词加标签
		String line = value.toString();
		String[] words = line.split("\t");
		//循环遍历打标记
		for (String string : words) {
			context.write(new Text(string), new IntWritable(1));
		}
	}
}
