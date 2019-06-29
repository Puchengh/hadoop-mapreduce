package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

@LimitedPrivate({"MapReduce", "Pig"})
@Evolving
public class LineRecordReader extends RecordReader<LongWritable, Text> {
	private static final Log LOG = LogFactory.getLog(LineRecordReader.class);
	public static final String MAX_LINE_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";
	private long start;
	private long pos;
	//设置偏移量
	private long end;
	private SplitLineReader in;
	private FSDataInputStream fileIn;
	private Seekable filePosition;
	private int maxLineLength;
	private LongWritable key;
	private Text value;
	private boolean isCompressedInput;
	private Decompressor decompressor;
	private byte[] recordDelimiterBytes;

	public LineRecordReader() {
	}

	public LineRecordReader(byte[] recordDelimiter) {
		this.recordDelimiterBytes = recordDelimiter;
	}

	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength", Integer.MAX_VALUE);
		this.start = split.getStart();
		this.end = this.start + split.getLength();
		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(job);
		this.fileIn = fs.open(file);
		CompressionCodec codec = (new CompressionCodecFactory(job)).getCodec(file);
		if (null != codec) {
			this.isCompressedInput = true;
			this.decompressor = CodecPool.getDecompressor(codec);
			if (codec instanceof SplittableCompressionCodec) {
				SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec).createInputStream(this.fileIn,
						this.decompressor, this.start, this.end, READ_MODE.BYBLOCK);
				this.in = new CompressedSplitLineReader(cIn, job, this.recordDelimiterBytes);
				this.start = cIn.getAdjustedStart();
				this.end = cIn.getAdjustedEnd();
				this.filePosition = cIn;
			} else {
				if (this.start != 0L) {
					throw new IOException("Cannot seek in " + codec.getClass().getSimpleName() + " compressed stream");
				}

				this.in = new SplitLineReader(codec.createInputStream(this.fileIn, this.decompressor), job,
						this.recordDelimiterBytes);
				this.filePosition = this.fileIn;
			}
		} else {
			this.fileIn.seek(this.start);
			this.in = new UncompressedSplitLineReader(this.fileIn, job, this.recordDelimiterBytes, split.getLength());
			this.filePosition = this.fileIn;
		}

		if (this.start != 0L) {
			this.start += (long) this.in.readLine(new Text(), 0, this.maxBytesToConsume(this.start));
		}

		this.pos = this.start;
	}

	private int maxBytesToConsume(long pos) {
		return this.isCompressedInput
				? Integer.MAX_VALUE
				: (int) Math.max(Math.min(2147483647L, this.end - pos), (long) this.maxLineLength);
	}

	private long getFilePosition() throws IOException {
		long retVal;
		if (this.isCompressedInput && null != this.filePosition) {
			retVal = this.filePosition.getPos();
		} else {
			retVal = this.pos;
		}

		return retVal;
	}

	private int skipUtfByteOrderMark() throws IOException {
		int newMaxLineLength = (int) Math.min(3L + (long) this.maxLineLength, 2147483647L);
		int newSize = this.in.readLine(this.value, newMaxLineLength, this.maxBytesToConsume(this.pos));
		this.pos += (long) newSize;
		int textLength = this.value.getLength();
		byte[] textBytes = this.value.getBytes();
		if (textLength >= 3 && textBytes[0] == -17 && textBytes[1] == -69 && textBytes[2] == -65) {
			LOG.info("Found UTF-8 BOM and skipped it");
			textLength -= 3;
			newSize -= 3;
			if (textLength > 0) {
				textBytes = this.value.copyBytes();
				this.value.set(textBytes, 3, textLength);
			} else {
				this.value.clear();
			}
		}

		return newSize;
	}

	public boolean nextKeyValue() throws IOException {
		if (this.key == null) {
			this.key = new LongWritable();
		}

		//pos记录总偏移量
		this.key.set(this.pos);
		
		if (this.value == null) {
			this.value = new Text();
		}

		//pos记录一行的偏移量
		int newSize = 0;
		//如果文件没有读到末尾   或者 还有别的切片的时候需要读取
		while (this.getFilePosition() <= this.end || this.in.needAdditionalRecordAfterSplit()) {
			if (this.pos == 0L) {
				newSize = this.skipUtfByteOrderMark();
			} else {
				newSize = this.in.readLine(this.value, this.maxLineLength, this.maxBytesToConsume(this.pos));
				this.pos += (long) newSize;
			}

			if (newSize == 0 || newSize < this.maxLineLength) {
				break;
			}

			LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - (long) newSize));
		}

		if (newSize == 0) {
			this.key = null;
			this.value = null;
			return false;
		} else {
			return true;
		}
	}

	public LongWritable getCurrentKey() {
		return this.key;
	}

	public Text getCurrentValue() {
		return this.value;
	}

	public float getProgress() throws IOException {
		return this.start == this.end
				? 0.0F
				: Math.min(1.0F, (float) (this.getFilePosition() - this.start) / (float) (this.end - this.start));
	}

	public synchronized void close() throws IOException {
		try {
			if (this.in != null) {
				this.in.close();
			}
		} finally {
			if (this.decompressor != null) {
				CodecPool.returnDecompressor(this.decompressor);
				this.decompressor = null;
			}

		}

	}
}