/**  
* Title: MergeFileRecordReader.java  
* Description:   
* @author Puchen  
* @date 2019年6月29日 下午3:23:46  
* @version 1.0  
*/  
package com.puchen.cn.input;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.ctc.wstx.io.ISOLatinReader;

/**  
* <p>Title: MergeFileRecordReader</p>  
* <p>Description: </p>  
* @author puchen  
* @date 2019年6月29日  下午3:23:46
*/
public class MergeFileRecordReader extends RecordReader<Text, NullWritable> {

	/**
	 * 主要是进行文件读取  整个文件进行读取  创建一个流 要获取一个文件路径
	 */

	FSDataInputStream open = null;
	//表示文件是否读取完成
	boolean isReader = false;
	Text k = new Text();
	long len = 0;
	
	//进行一些链接或者流的关闭
	@Override
	public void close() throws IOException {
		open.close();
	}


	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return this.k;
	}


	@Override
	public NullWritable getCurrentValue() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	//获取执行进度的方法  文件读取了多少
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return isReader?1.0F:0.0F;
	}

	//初始化方法  类似于setUp  用于对属性进行或者链接或流初始化
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit sl = (FileSplit)split;
		Path path = sl.getPath();
		//通过路劲  获取流
		FileSystem fs = FileSystem.get(context.getConfiguration());
		//创建输入流
		open = fs.open(path);
		len = sl.getLength();
	}

	//文件是否读取结束
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!isReader) {
			byte[] b=  new byte[(int)len];
			open.readFully(0,b);
			k.set(b);
			isReader = true;
			return isReader;
		}else {
			return false;
		}
	}

}
