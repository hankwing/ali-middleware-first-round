package com.alibaba.middleware.tools;

import java.util.ArrayList;

public class Test {
	
	public static void main( String[] args) {
		ArrayList<Long> array = new ArrayList<Long>();
		array.add(2L);
		array.add(4L);
		array.add(10L);
		array.add(1L);
		array.sort(null);
		System.out.println(System.currentTimeMillis());
	}
}
