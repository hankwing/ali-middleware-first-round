package com.alibaba.middleware.tools;

public class PartialResult extends Object implements Cloneable{

	public double tmallTrade  = 0;
	public double taobaoTrade = 0;
	public double PC = 0;
	public double mobile = 0;
	public long time = 0;
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}

	public PartialResult( long time, double tmallTrade, 
			double taobaoTrade, double PC, double Mobile) {
		this.time = time;
		this.tmallTrade = tmallTrade;
		this.taobaoTrade = taobaoTrade;
		this.PC = PC;
		this.mobile = Mobile;
	}
	
	public PartialResult( long time) {
		this.time = time;
		this.tmallTrade = 0;
		this.taobaoTrade = 0;
		this.PC = 0;
		this.mobile = 0;
	}
	
	public PartialResult() {}

}
