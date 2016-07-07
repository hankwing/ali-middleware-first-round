package com.alibaba.middleware.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import com.alibaba.middleware.race.RaceConfig;
import com.esotericsoftware.minlog.Log;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

public class Test {
	
	public static void main( String[] args) {
		
		Double temp = 0.0;
		double temp2 = 0;
		if( temp.doubleValue() == temp2) {
			System.out.println("heh");
		}
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
		confServers.add(RaceConfig.TairConfigServer);
		confServers.add(RaceConfig.TairSalveConfigServer);

		// 创建客户端实例
		DefaultTairManager tairManager = new DefaultTairManager();
		tairManager.setConfigServerList(confServers);

		// 设置组名
		tairManager.setGroupName("group_1");
		// 初始化客户端
		tairManager.init();
		
		ResultCode rc1 = tairManager.put(0, RaceConfig.prex_tmall + 0, "12345");
		rc1 = tairManager.put(0, RaceConfig.prex_tmall + 0, "123456");
		if( rc1.isSuccess()) {
			System.out.println("tair write success!");
			DataEntry resultTmall = tairManager.get(0, RaceConfig.prex_tmall + 0).getValue();
			System.out.println(resultTmall.getValue());
		}*/
		/*for( long time = 1466326740; time < 1466337361; time += 60) {
			
			DataEntry resultTmall = tairManager.get(0, RaceConfig.prex_tmall + time).getValue();
			DataEntry resultTaobao = tairManager.get(0, RaceConfig.prex_taobao + time).getValue();
			DataEntry resultMobile = tairManager.get(0, RaceConfig.prex_mobile + time).getValue();
			DataEntry resultPC = tairManager.get(0, RaceConfig.prex_pc + time).getValue();
			DataEntry resultRatio = tairManager.get(0, RaceConfig.prex_ratio + time).getValue();
			
			System.out.println(resultTmall.getValue());
			System.out.println(resultTaobao.getValue());
			System.out.println(resultMobile.getValue());
			System.out.println(resultPC.getValue());
			System.out.println(resultRatio.getValue() + "\n");
		}*/

		
		/*Timer timer  = new Timer();
		timer.schedule(new TimerTask() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				System.out.println("timer");
			}
			
		}, 10000);*/

	}
}
