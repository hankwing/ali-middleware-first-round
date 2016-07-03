package com.alibaba.middleware.components;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.tools.PartialResult;
import com.esotericsoftware.minlog.Log;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
/**
 * 将partialResultsBolt的结果汇总 输出总结果
 * @author hankwing
 *
 */
public class MergeResultsBolt implements IBasicBolt {

	private static final long serialVersionUID = -3460430184773833659L;
	private static final Logger LOG = LoggerFactory.getLogger(MergeResultsBolt.class);
	private Writer writer = null;
	private FileOutputStream fos = null;
	private TopologyContext context = null;
	private Map<Long, List<PartialResult>> resultList = null;
	private int numberOfPartialResults = 0;
	private Timer cleanupTimer = null;
	private int numSlots = 0;
	private DefaultTairManager tairManager = null;
	
	public MergeResultsBolt( int numSlots) {
		this.numSlots = numSlots;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		this.context = context;
		resultList = new HashMap<Long, List<PartialResult>>();
		numberOfPartialResults = context.getComponentTasks
				(RaceConfig.ComponentPartialResultBolt).size();
		
		List<String> confServers = new ArrayList<String>();
		confServers.add(RaceConfig.TairConfigServer);
		confServers.add(RaceConfig.TairSalveConfigServer);

		// 创建客户端实例
		tairManager = new DefaultTairManager();
		tairManager.setConfigServerList(confServers);

		// 设置组名
		tairManager.setGroupName(RaceConfig.TairGroup);
		 //初始化客户端
		tairManager.init();
		
		try {
			fos = new FileOutputStream("result");
			writer = new BufferedWriter(
					new OutputStreamWriter(fos, "utf-8"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		cleanupTimer = new Timer();
		cleanupTimer.schedule(new TimerTask() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				LOG.info("call mergeresult cleanupTimer");
				slidingReaminderWindow();
			}
			
		}, 16*60*1000);
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		/*PartialResult result = new PartialResult(input.getLongByField("time"),
				input.getDoubleByField("tmallTrade"), input.getDoubleByField("taobaoTrade")
				,input.getDoubleByField("PC"), input.getDoubleByField("Mobile"));*/
		PartialResult result = (PartialResult) input.getValueByField("partialResult");
		List<PartialResult> partResults = resultList.get(result.time);
		if( partResults == null) {
			partResults = new ArrayList<PartialResult>();
			resultList.put(result.time, partResults);
		}
		partResults.add(result);
		
		if( partResults.size() >= numberOfPartialResults) {
			// get results of the minute
			Double tmallTrade = 0.0;
			Double taobaoTrade = 0.0;
			Double PC = 0.0;
			Double Mobile = 0.0;
			for(PartialResult temp : partResults) {
				tmallTrade += temp.tmallTrade;
				taobaoTrade += temp.taobaoTrade;
				PC += temp.PC;
				Mobile += temp.mobile;
			}
			//try {
				Long time = result.time * 60;
				/*writer.write("key: " + RaceConfig.prex_tmall + 
						time + " value:" + String.format("%.2f",tmallTrade) + "\n");
				writer.write("key: " + RaceConfig.prex_taobao + 
						time + " value:" + String.format("%.2f",taobaoTrade) + "\n");
				writer.write("key: " + RaceConfig.prex_ratio + 
						time + "mobile_value:" + String.format("%.2f",Mobile) + "\n");
				writer.write("key: " + RaceConfig.prex_ratio + 
						time + "pc_value:" + String.format("%.2f",PC) + "\n");
				writer.write("key: " + RaceConfig.prex_ratio + 
						time + " value:" + String.format("%.2f",Mobile / PC) + "\n\n");
				
				writer.flush();*/
				
				ResultCode rc1 = tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_tmall + 
						time, String.format("%.2f",tmallTrade));
				ResultCode rc2 = tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_taobao + 
						time, String.format("%.2f",taobaoTrade));
				ResultCode rc3 = tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + 
						time, String.format("%.2f",Mobile / PC));
				if (rc1.isSuccess() && rc2.isSuccess() && rc3.isSuccess()) {
				    // put成功
					LOG.info("tair success!!, time:{}, values:{},{},{}",result.time, 
							String.format("%.2f",tmallTrade, String.format("%.2f",taobaoTrade),
									String.format("%.2f",Mobile / PC)));
				} else if (ResultCode.VERERROR.equals(rc1)) {
				    // 版本错误的处理代码
					LOG.info("tair failed because version error!!:");
				} else {
				    // 其他失败的处理代码
					LOG.info("tair failed because other reasons!!:");
				}
				
			/*} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
			
			resultList.remove(result.time);
			
		}
	}
	
	public void slidingReaminderWindow() {
		try {
			LOG.info( "Mergeresult bolt clean up !!!!!!!!!");
			for( List<PartialResult> partResults : resultList.values()) {
				LOG.info( "Mergeresult bolt clean up !!!!!!!!!");
				Double tmallTrade = 0.0;
				Double taobaoTrade = 0.0;
				Double PC = 0.0;
				Double Mobile = 0.0;
				for(PartialResult temp : partResults) {
					tmallTrade += temp.tmallTrade;
					taobaoTrade += temp.taobaoTrade;
					PC += temp.PC;
					Mobile += temp.mobile;
				}
				//try {
					Long time = partResults.get(0).time * 60;
					/*writer.write("key: " + RaceConfig.prex_tmall + 
							time + " value:" + String.format("%.2f",tmallTrade) + "\n");
					writer.write("key: " + RaceConfig.prex_taobao + 
							time + " value:" + String.format("%.2f",taobaoTrade) + "\n");
					writer.write("key: " + RaceConfig.prex_ratio + 
							time + "mobile_value:" + String.format("%.2f",Mobile) + "\n");
					writer.write("key: " + RaceConfig.prex_ratio + 
							time + "pc_value:" + String.format("%.2f",PC) + "\n");
					writer.write("key: " + RaceConfig.prex_ratio + 
							time + " value:" + String.format("%.2f",Mobile / PC) + "\n\n");
					
					writer.flush();*/
					
					ResultCode rc1 = tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_tmall + 
							time, String.format("%.2f",tmallTrade));
					ResultCode rc2 = tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_taobao + 
							time, String.format("%.2f",taobaoTrade));
					ResultCode rc3 = tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio + 
							time, String.format("%.2f",Mobile / PC));
					if (rc1.isSuccess() && rc2.isSuccess() && rc3.isSuccess()) {
					    // put成功
						LOG.info("cleanup tair success!!:");
					} else if (ResultCode.VERERROR.equals(rc1)) {
					    // 版本错误的处理代码
						LOG.info("tair failed because version error!!:");
					} else {
					    // 其他失败的处理代码
						LOG.info("tair failed because other reasons!!:");
					}
				/*} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
			}
			writer.flush();
			writer.close();
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
		
	}

}
