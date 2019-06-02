package com.puchen.cn.DefineClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{

	
	private int upoflow;
	private int downflow;
	private int sumflow;
	
	public int getUpoflow() {
		return upoflow;
	}
	public void setUpoflow(int upoflow) {
		this.upoflow = upoflow;
	}
	public int getDownflow() {
		return downflow;
	}
	public void setDownflow(int downflow) {
		this.downflow = downflow;
	}
	public int getSumflow() {
		return sumflow;
	}
	public void setSumflow(int sumflow) {
		this.sumflow = sumflow;
	}
	@Override
	public String toString() {
		return upoflow + "\t"+downflow+ "\t"+sumflow+ "\t";
	}

	public FlowBean() {
		super();
	}
	
	public FlowBean(int upoflow, int downflow) {
		super();
		this.upoflow = upoflow;
		this.downflow = downflow;
		this.sumflow = this.upoflow+this.downflow;
	}
	//反序列化的方法
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.upoflow = in.readInt();
		this.downflow = in.readInt();
		this.sumflow = in.readInt();
		
	}
	//序列化的方法
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		//每个属性都需要裂化
		out.writeInt(upoflow);
		out.writeInt(downflow);
		out.writeInt(sumflow);
		
	}
	
	
	
	
}
