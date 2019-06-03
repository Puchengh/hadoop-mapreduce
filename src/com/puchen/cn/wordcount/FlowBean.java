package com.puchen.cn.wordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{

	private String phoneNum;
	private int upflow;
	private int downflow;
	private int sunflow;
	public String getPhoneNum() {
		return phoneNum;
	}
	public void setPhoneNum(String phoneNum) {
		this.phoneNum = phoneNum;
	}
	public int getUpflow() {
		return upflow;
	}
	public void setUpflow(int upflow) {
		this.upflow = upflow;
	}
	public int getDownflow() {
		return downflow;
	}
	public void setDownflow(int downflow) {
		this.downflow = downflow;
	}
	public int getSunflow() {
		return sunflow;
	}
	public void setSunflow(int sunflow) {
		this.sunflow = sunflow;
	}
	public FlowBean() {
		super();
		// TODO Auto-generated constructor stub
	}
	public FlowBean(String phoneNum, int upflow, int downflow, int sunflow) {
		super();
		this.phoneNum = phoneNum;
		this.upflow = upflow;
		this.downflow = downflow;
		this.sunflow = sunflow;
	}
	@Override
	public String toString() {
		return phoneNum + "\t"+upflow + "\t"+downflow + "\t"+sunflow + "\t";
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.phoneNum = in.readUTF();
		this.upflow = in.readInt();
		this.downflow = in.readInt();
		this.sunflow = in.readInt();
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(phoneNum);
		out.writeInt(upflow);
		out.writeInt(downflow);
		out.writeInt(sunflow);
	}
	
	//map-reduce的比较规则
	@Override
	public int compareTo(FlowBean o) {
		// TODO Auto-generated method stub
		return o.getSunflow()-this.getSunflow();
	}
	
}
