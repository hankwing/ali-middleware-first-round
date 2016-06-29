package com.alibaba.middleware.tools;

public class PartialResult extends Object implements Cloneable{

	public double tmallTrade  = 0;
	public double taobaoTrade = 0;
	public double PC = 0;
	public double mobile = 0;
	public Long time = 0L;
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}

	public PartialResult( Long time, double tmallTrade, 
			double taobaoTrade, double PC, double Mobile) {
		this.time = time;
		this.tmallTrade = tmallTrade;
		this.taobaoTrade = taobaoTrade;
		this.PC = PC;
		this.mobile = Mobile;
	}
	
	public PartialResult( Long time) {
		this.time = time;
	}
	
	public PartialResult() {}

}
