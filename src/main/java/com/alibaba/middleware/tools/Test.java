package com.alibaba.middleware.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import com.alibaba.middleware.race.RaceConfig;
import com.esotericsoftware.minlog.Log;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

public class Test {
	
	public static void main( String[] args) {
		
		/*List<String> confServers = new ArrayList<String>();
		confServers.add("192.168.52.128:5198"); 
	//	confServers.add("10.10.7.144:51980"); // 可选

		// 创建客户端实例
		DefaultTairManager tairManager = new DefaultTairManager();
		tairManager.setConfigServerList(confServers);

		// 设置组名
		tairManager.setGroupName("group_1");
		// 初始化客户端
		tairManager.init();*/
		
		
		/*List<String> confServers = new ArrayList<String>();
		confServers.add("192.168.52.128:5198");

		// 创建客户端实例
		DefaultTairManager tairManager = new DefaultTairManager();
		tairManager.setConfigServerList(confServers);

		// 设置组名
		tairManager.setGroupName("group_1");
		// 初始化客户端
		tairManager.init();*/
		
		Timer timer  = new Timer();
		timer.schedule(new TimerTask() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				System.out.println("timer");
			}
			
		}, 10000);

	}
}
