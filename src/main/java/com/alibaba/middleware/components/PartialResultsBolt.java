package com.alibaba.middleware.components;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceConfig.TradeType;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.tools.PartialResult;
import com.alibaba.middleware.tools.SlidingWindowCounter;

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
	private SlidingWindowCounter counter = null;
	private Map<Long, Map<Long,OrderToBeProcess>> ordersToBeProcess = null;
	private Map<Long, List<Long>> tmallOrders = null;
	private Map<Long, List<Long>> taobaoOrders = null;
	private BasicOutputCollector _collector = null;

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
		System.out.println("numSlots: " + RaceConfig.windowLengthInSeconds);
		ordersToBeProcess = new HashMap<Long, Map<Long,OrderToBeProcess>>();
		tmallOrders = new HashMap<Long, List<Long>>();
		taobaoOrders = new HashMap<Long, List<Long>>();
	}

	private int deriveNumWindowChunksFrom(int windowLengthInSeconds,
			int windowUpdateFrequencyInSeconds) {
		return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		// 3 types of input: tmall, taobao, payment
		_collector = collector;
		String componentID = input.getSourceComponent();
		Long time = input.getLongByField("createTime");
		if (componentID.equals(RaceConfig.ComponentTmallSpout)) {
			// execute tmall order tuple
			Long tmallOrderID = input.getLongByField("orderID");
			boolean isFound = false;
			for( Map<Long,OrderToBeProcess> list: ordersToBeProcess.values()) {
				OrderToBeProcess order = list.get(tmallOrderID);
				if (order != null) {
					// find the tmall order in the payment list
					isFound = true;
					counter.incrementCount(order.time, TradeType.Tmall,
							order.payAmount);
					// delete from list
					list.remove(tmallOrderID);
					break;
				}
			}
			
			if( !isFound) {
				List<Long> ordersList = tmallOrders.get(time);
				if( ordersList == null) {
					ordersList = new ArrayList<Long>();
					tmallOrders.put(time, ordersList);
				}
				ordersList.add(tmallOrderID);
			}
			
		} else if (componentID.equals(RaceConfig.ComponentTaobaoSpout)) {
			// execute taobao order tuple
			Long taobaoOrderID = input.getLongByField("orderID");
			
			boolean isFound = false;
			for( Map<Long,OrderToBeProcess> list: ordersToBeProcess.values()) {
				OrderToBeProcess order = list.get(taobaoOrderID);
				if (order != null) {
					// find the tmall order in the payment list
					isFound = true;
					counter.incrementCount(order.time, TradeType.Taobao,
							order.payAmount);
					// delete from list
					list.remove(taobaoOrderID);
					break;
				}
			}
			
			if( !isFound) {
				List<Long> ordersList = taobaoOrders.get(time);
				if( ordersList == null) {
					ordersList = new ArrayList<Long>();
					taobaoOrders.put(time, ordersList);
				}
				ordersList.add(taobaoOrderID);
			}

		} else if (componentID.equals(RaceConfig.ComponentPaymentSpout)) {
			// execute payment tuple
			Long orderID = input.getLongByField("orderID");
			double payAmount = input.getDoubleByField("payAmount");
			boolean isFound = false;
			TradeType type = input.getShortByField("payPlatform") == 0 ? TradeType.PC
					: TradeType.Mobile;
			counter.incrementCount(time, type,payAmount);

			for( List<Long> tmall : tmallOrders.values()) {
				if( tmall.contains(orderID)) {
					counter.incrementCount(time, TradeType.Tmall,payAmount);
					//tmall.remove(orderID);
					isFound = true;
					break;
				}
			}
			if( !isFound) {
				for( List<Long> taobao : taobaoOrders.values()) {
					if( taobao.contains(orderID)) {
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
					tmallOrders.remove(result.time);	// remove expired orders
					taobaoOrders.remove(result.time);
				}
			}
			
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		for( PartialResult temp: counter.getRemainResults()) {
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
