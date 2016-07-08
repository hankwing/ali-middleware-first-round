package com.alibaba.middleware.components;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceConfig.TradeType;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.tools.PartialResult;
import com.alibaba.middleware.tools.SlidingWindowCounter;
import com.esotericsoftware.minlog.Log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 处理从TmallSpout\TaobaoSpout\PaySpout发出的数据
 * @author hankwing
 *
 */
public class PartialResultsBolt implements IRichBolt {

	private static final long serialVersionUID = -7776452677749510415L;
	private static final Logger Log = LoggerFactory.getLogger(PartialResultsBolt.class);
	private SlidingWindowCounter counter = null;
	private Map<Long,OrderToBeProcess> ordersToBeProcess = null;
	private Map<Long, Double> tmallOrders = null;
	private Map<Long, Double> taobaoOrders = null;
	private OutputCollector _collector = null;
	
	private Timer cleanupTimer = null;
	private boolean isEnd = true;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("partialResult"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}


	private int deriveNumWindowChunksFrom(int windowLengthInSeconds,
			int windowUpdateFrequencyInSeconds) {
		return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
	}
	
	public void slidingReaminderWindow() {
		Log.info( "partialResult bolt clean up start!!!!!!!!!");
		for( PartialResult temp: counter.getRemainResults()) {
			Log.info("emit remaining partial result time:{}", temp.time);
			_collector.emit(new Values(temp));
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		/*for( PartialResult temp: counter.getRemainResults()) {
			Log.info( "partial result bolt clean up !!!!!!!!!");
			_collector.emit(new Values(temp));
		}*/
		
	}

	public static class OrderToBeProcess {

		public List<Double> payList = null;
		public List<Long> times = null;

		public OrderToBeProcess(long time, double payAmount) {
			payList = new ArrayList<Double>();
			times = new ArrayList<Long>();
			times.add(time);
			payList.add(payAmount);
		}
		
		public boolean addPayAmount( long time, double part) {
			if( payList.contains(part) && times.contains(time)) {
				return false;
				
			}
			else {
				times.add(time);
				payList.add(part);
				return true;
			}
			
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this._collector = collector;
		counter = new SlidingWindowCounter(deriveNumWindowChunksFrom(
				RaceConfig.windowLengthInSeconds, RaceConfig.emitFrequencyInSeconds));
		//System.out.println("numSlots: " + RaceConfig.windowLengthInSeconds);
		ordersToBeProcess = new HashMap<Long,OrderToBeProcess>();
		tmallOrders = new HashMap<Long,Double>();
		taobaoOrders = new HashMap<Long,Double>();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		if(isEnd) {
			// mark the bolt is continuing
			if(cleanupTimer == null ) {
				// check every 30 secs
				cleanupTimer = new Timer();
				cleanupTimer.schedule(new TimerTask() {

					@Override
					public void run() {
						// TODO Auto-generated method stub
						if(isEnd) {
							Log.info("call partial result cleanupTimer,remaining tmall orders:{},"
									+ "remaining taobao orders:{}",tmallOrders.size(),taobaoOrders.size());
							slidingReaminderWindow();
							// then cancel the timer
							this.cancel();
						}
						isEnd = true;
						
					}
					
				},30*1000, 30*1000);
			}
			isEnd = false;
		}
		
		String topic = input.getString(0);
		long mtime = input.getLong(1);
		long time = mtime / 1000 / 60;
		double payAmount = input.getDouble(3);
		
		/*Log.info("Time:{} ,TmallorderCount is {}, TaobaoOrderCount is {}, PayOrderCount is {}", 
				time*60,
				tmallOrders.get(time) != null? tmallOrders.get(time).size() : 0, 
				taobaoOrders.get(time) != null? taobaoOrders.get(time).size() : 0, 
				ordersToBeProcess.get(time) != null?ordersToBeProcess.get(time).size(): 0);*/
		if (topic.equals(RaceConfig.MqTmallTradeTopic)) {
			// execute tmall order tuple
			long tmallOrderID = input.getLong(2);
			if( tmallOrders.put(tmallOrderID, payAmount) == null) {
				//boolean isFound = false;
				OrderToBeProcess order = ordersToBeProcess.get(tmallOrderID);
				if (order != null) {
					// find the tmall order in the payment list
					//isFound = true;
					//if( order.payAmount == payAmount ) {
						//tmallOrders.remove(tmallOrderID);
						//ordersToBeProcess.remove(tmallOrderID);
					//}
					for( int i = 0; i< order.times.size(); i++ ) {
						counter.incrementCount(order.times.get(i) / 1000 / 60, TradeType.Tmall,
								order.payList.get(i));
					}
					
				}
			}
			else {
				Log.error("duplicate Tmall message:{}", mtime);
			}
			
		} else if (topic.equals(RaceConfig.MqTaobaoTradeTopic)) {
			// execute taobao order tuple
			//Log.info("receive taobaoTuples");
			long taobaoOrderID = input.getLong(2);
			if( taobaoOrders.put(taobaoOrderID, payAmount) == null) {
				//boolean isFound = false;
				OrderToBeProcess order = ordersToBeProcess.get(taobaoOrderID);
				if (order != null) {
					// find the tmall order in the payment list
					//isFound = true;
					/*if( order.payAmount == payAmount ) {
						taobaoOrders.remove(taobaoOrderID);
						//ordersToBeProcess.remove(taobaoOrderID);
					}*/
					for( int i = 0; i< order.times.size(); i++ ) {
						counter.incrementCount(order.times.get(i) / 1000 / 60, TradeType.Taobao,
								order.payList.get(i));
					}
				}
			}
			else {
				Log.error("duplicate taobao message:{}", mtime);
			}

		} else if (topic.equals(RaceConfig.MqPayTopic)) {
			// execute payment tuple
			long orderID = input.getLong(2);
			
			OrderToBeProcess order = ordersToBeProcess.get(orderID);
			if( order == null) {
				ordersToBeProcess.put( orderID, new OrderToBeProcess(mtime,payAmount));
				TradeType type = input.getShort(4) == 0 ? TradeType.PC
						: TradeType.Mobile;
				counter.incrementCount(time, type, payAmount);
			}
			else if(order.addPayAmount(mtime, payAmount)){
				
				// need to merge result
				TradeType type = input.getShort(4) == 0 ? TradeType.PC
						: TradeType.Mobile;
				counter.incrementCount(time, type, payAmount);
			}
			else {
				// duplicate
				Log.error("duplicate payment message:{}:{}:{}:{}", mtime, payAmount,order.times,order.payList);
				_collector.ack(input);
				return;
			}
			
			if( tmallOrders.get(orderID) != null) {
				counter.incrementCount(time, TradeType.Tmall,payAmount);
			}
			else if( taobaoOrders.get(orderID) != null) {
				counter.incrementCount(time, TradeType.Taobao,payAmount);
			}
			
			
			/*if( counter.isNeedSlide()) {
				// need to send partial results tuples
				PartialResult result = counter.getSlidingPartialResult(ordersToBeProcess);
				
				if(result != null) {
					collector.emit(new Values(result));
					tmallOrders.remove(result.time - RaceConfig.orderExpiredMinutes);	// remove expired orders
					taobaoOrders.remove(result.time - RaceConfig.orderExpiredMinutes);
				}
			}*/
			
		}
		
		_collector.ack(input);
	}

}
