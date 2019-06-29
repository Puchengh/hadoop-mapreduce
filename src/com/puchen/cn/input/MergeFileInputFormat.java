/**  
* Title: MergeFileInputFormat.java  
* Description:   
* @author Puchen  
* @date 2019年6月29日 下午3:16:51  
* @version 1.0  
*/  
package com.puchen.cn.input;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**  
 * K:Mapper输入的是key的类型 默认值得是偏移量
 * V：Mapper输入的value的类型  默认值得是一行的内容
 * 每次读取一个文件的内容
 * key:整个文件的内容
 * value:NullWrite
 * 
* <p>Title: MergeFileInputFormat</p>  
* <p>Description: </p>  
* @author puchen  
* @date 2019年6月29日  下午3:16:51
*/
public class MergeFileInputFormat extends FileInputFormat<Text, NullWritable> {

	/**
	 * 构建recordReader
	 * InputSplit var1：文件切片   文件的切片信息
	 * TaskAttemptContext var2：上下文对象
	 */
	@Override
	public RecordReader<Text, NullWritable> createRecordReader(InputSplit var1, TaskAttemptContext var2)
			throws IOException, InterruptedException {
		MergeFileRecordReader record = new MergeFileRecordReader();
		record.initialize(var1, var2);
		return record;
	}

	
}
