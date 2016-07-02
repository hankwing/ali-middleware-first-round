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
public class PartialResultsBolt implements IBasicBolt {

	private static final long serialVersionUID = -7776452677749510415L;
	private static final Logger Log = LoggerFactory.getLogger(PartialResultsBolt.class);
	private SlidingWindowCounter counter = null;
	private Map<Long, Map<Long,OrderToBeProcess>> ordersToBeProcess = null;
	private Map<Long, Map<Long, Boolean>> tmallOrders = null;
	private Map<Long, Map<Long, Boolean>> taobaoOrders = null;
	private BasicOutputCollector _collector = null;
	
	private Timer cleanupTimer = null;

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

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		counter = new SlidingWindowCounter(deriveNumWindowChunksFrom(
				RaceConfig.windowLengthInSeconds, RaceConfig.emitFrequencyInSeconds));
		//System.out.println("numSlots: " + RaceConfig.windowLengthInSeconds);
		ordersToBeProcess = new HashMap<Long, Map<Long,OrderToBeProcess>>();
		tmallOrders = new HashMap<Long, Map<Long,Boolean>>();
		taobaoOrders = new HashMap<Long, Map<Long,Boolean>>();
		cleanupTimer = new Timer();
		cleanupTimer.schedule(new TimerTask() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				Log.info("call cleanupTimer");
				slidingReaminderWindow();
			}
			
		}, 14*60*1000);
	}

	private int deriveNumWindowChunksFrom(int windowLengthInSeconds,
			int windowUpdateFrequencyInSeconds) {
		return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		// 3 types of input: tmall, taobao, payment
		if( _collector == null) {
			_collector = collector;
		}
		
		String topic = input.getStringByField("topic");
		Long time = input.getLongByField("createTime");
		
		/*Log.info("Time:{} ,TmallorderCount is {}, TaobaoOrderCount is {}, PayOrderCount is {}", 
				time*60,
				tmallOrders.get(time) != null? tmallOrders.get(time).size() : 0, 
				taobaoOrders.get(time) != null? taobaoOrders.get(time).size() : 0, 
				ordersToBeProcess.get(time) != null?ordersToBeProcess.get(time).size(): 0);*/
		if (topic.equals(RaceConfig.MqTmallTradeTopic)) {
			// execute tmall order tuple
			//Log.info("receive tmallTuples");
			Long tmallOrderID = input.getLongByField("orderID");
			//boolean isFound = false;
			for( Map<Long,OrderToBeProcess> list: ordersToBeProcess.values()) {
				OrderToBeProcess order = list.get(tmallOrderID);
				if (order != null) {
					// find the tmall order in the payment list
					//isFound = true;
					counter.incrementCount(order.time, TradeType.Tmall,
							order.payAmount);
					// delete from list
					list.remove(tmallOrderID);
					break;
				}
			}
			// add the order id to tmall list
			Map<Long,Boolean> ordersList = tmallOrders.get(time);
			if( ordersList == null) {
				ordersList = new HashMap<Long, Boolean>();
				tmallOrders.put(time, ordersList);
			}
			ordersList.put(tmallOrderID, true);
			
		} else if (topic.equals(RaceConfig.MqTaobaoTradeTopic)) {
			// execute taobao order tuple
			//Log.info("receive taobaoTuples");
			Long taobaoOrderID = input.getLongByField("orderID");

			for( Map<Long,OrderToBeProcess> list: ordersToBeProcess.values()) {
				OrderToBeProcess order = list.get(taobaoOrderID);
				if (order != null) {
					// find the tmall order in the payment list
					counter.incrementCount(order.time, TradeType.Taobao,
							order.payAmount);
					// delete from list
					list.remove(taobaoOrderID);
					break;
				}
			}
			Map<Long, Boolean> ordersList = taobaoOrders.get(time);
			if( ordersList == null) {
				ordersList = new HashMap<Long, Boolean>();
				taobaoOrders.put(time, ordersList);
			}
			ordersList.put(taobaoOrderID, true);

		} else if (topic.equals(RaceConfig.MqPayTopic)) {
			// execute payment tuple
			Long orderID = input.getLongByField("orderID");
			
			double payAmount = input.getDoubleByField("payAmount");
			boolean isFound = false;
			TradeType type = input.getShortByField("payPlatform") == 0 ? TradeType.PC
					: TradeType.Mobile;
			counter.incrementCount(time, type, payAmount);

			for( Map<Long, Boolean> tmall : tmallOrders.values()) {
				if( tmall.get(orderID) != null) {
					counter.incrementCount(time, TradeType.Tmall,payAmount);
					//tmall.remove(orderID);
					isFound = true;
					break;
				}
			}
			if( !isFound) {
				for( Map<Long, Boolean> taobao : taobaoOrders.values()) {
					//System.out.println("orderID:" + orderID);
					if( taobao.get(orderID) != null) {
						counter.incrementCount(time, TradeType.Taobao,payAmount);
						//taobao.remove(orderID);
						isFound = true;
						break;
					}
				}
			}
			if( !isFound) {
				
				Map<Long, OrderToBeProcess> addList = ordersToBeProcess.get(time);
				if(addList == null) {
					addList = new HashMap<Long, OrderToBeProcess>();
					ordersToBeProcess.put(time, addList);
				}
				OrderToBeProcess order = addList.get(orderID);
				if( order == null) {
					addList.put( orderID, new OrderToBeProcess(time,payAmount));
				}
				else {
					// need to merge result
					order.addPayAmount(payAmount);
				}
			}
			if( counter.isNeedSlide()) {
				// need to send partial results tuples
				PartialResult result = counter.getSlidingPartialResult(ordersToBeProcess);
				
				if(result != null) {
					collector.emit(new Values(result));
					tmallOrders.remove(result.time - RaceConfig.orderExpiredMinutes);	// remove expired orders
					taobaoOrders.remove(result.time - RaceConfig.orderExpiredMinutes);
				}
			}
			
		}
	}
	
	public void slidingReaminderWindow() {
		Log.info( "Mergeresult bolt clean up !!!!!!!!!");
		for( PartialResult temp: counter.getRemainResults()) {
			Log.info( "partial result bolt clean up !!!!!!!!!");
			_collector.emit(new Values(temp));
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		for( PartialResult temp: counter.getRemainResults()) {
			Log.info( "partial result bolt clean up !!!!!!!!!");
			_collector.emit(new Values(temp));
		}
		
	}

	public static class OrderToBeProcess {

		public Long time = 0L;
		public double payAmount = 0;

		public OrderToBeProcess(Long time, double payAmount) {
			this.time = time;
			this.payAmount = payAmount;
		}
		
		public void addPayAmount( double part) {
			payAmount += part;
		}
	}

}
