package com.puchen.cn.partition;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * reduce处理的是map的结果  reduce的输入时map的输出
 * KEYIN：reduce输入的类型  ---Mapper输出的key的类型  Text
 * VALUEIN：Reduce输入的value的类型  ---Mapper输出的value的类型 IntWritable
 * 输出的reduce最终处理完的业务逻辑的类型   hello，67
 * KEYOUT：reduce统计加过的keu的类型  这里指的是最终统计完成的单词Text
 * VALUEOUT：reduc统计结果的value的类型  这里指的是单词出现的总次数  IntWritable
 * @author Administrator
 *
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

	/**
	 * map端输出的结果：
	 * <hello,1> <spark,1> <hello,1>
	 * reduce端想要对这种结果进行统计  最好相同的单词在一起  事实上确实是在一起
	 * map端输出的数据到达reduce端之前就会对数据进行一个整理  这个整理是框架做的  这个整理就是分组
	 * 框架会进行一个分组  按照map输出的key进行分组  key相同的胃一组  map端输出有多少各不同的keu就有多少个组
	 * Text key：每一组中相同的key
	 * Iterable<IntWritable> value,每一组中相同key对应的所有value值<1,1,1,1,1>
	 * Context context:上下文 用于传输  写到hdfs
	 * hadoop
	 * 
	 *     这个方法的电泳频率：每一组调用一次
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		// 统计结果  循环遍历values求和
		int sum = 0;
		for (IntWritable intWritable : values) {
			sum+=intWritable.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
