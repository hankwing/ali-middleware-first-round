package com.alibaba.middleware.components;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.KillOptions;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.tools.MetaTuple;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

public class PaySpout implements IRichSpout,MessageListenerConcurrently {
	/**  */
	private static final long serialVersionUID = 8476906628618859716L;
	private static final Logger LOG = LoggerFactory.getLogger(PaySpout.class);

	// protected DefaultMQPushConsumer metaClientConfig;
	protected SpoutOutputCollector collector;
	protected transient DefaultMQPushConsumer consumer;

	protected Map conf;
	protected String id;
	protected boolean flowControl;
	protected boolean autoAck;
	private int suicide = 0;

	protected transient LinkedBlockingDeque<MetaTuple> sendingQueue;

	public PaySpout() {

	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.conf = conf;
		this.collector = collector;
		this.sendingQueue = new LinkedBlockingDeque<MetaTuple>();
		
		flowControl = false;
		autoAck = true;
		// initMetricClient(context);

		try {
			consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
			//consumer.setNamesrvAddr(RaceConfig.MqConfigServer);	// need to delete when in competition
			
			consumer.subscribe(RaceConfig.MqPayTopic, "*");
			consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
			consumer.registerMessageListener(this);
			consumer.setConsumeMessageBatchMaxSize(100);
			consumer.start();
		} catch (Exception e) {
			LOG.error("Failed to create Meta Consumer ", e);
			throw new RuntimeException("Failed to create MetaConsumer" + id, e);
		}

		LOG.info("Successfully init " + id);

	}

	@Override
	public void close() {
		if (consumer != null) {
			consumer.shutdown();
		}
	}

	@Override
	public void activate() {
		if (consumer != null) {
			consumer.resume();
		}

	}

	@Override
	public void deactivate() {
		if (consumer != null) {
			consumer.suspend();
		}
	}

	public void sendTuple(MetaTuple metaTuple) {
		metaTuple.updateEmitMs();
		
		for (MessageExt me : metaTuple.getMsgList()) {
			// 消费每条消息，如果消费失败，比如更新数据库失败，就重新再拉一次消息
			
			String topic = me.getTopic();
			byte[] body = me.getBody();
			if ( body!= null && body.length == 2 && body[0] == 0
					&& body[1] == 0) {
				// Info: 生产者停止生成数据, 并不意味着马上结束
				suicide ++;
				LOG.info("receive stop signs:{}, {}times", body, suicide );
				/*if(false) {
					Map conf = Utils.readStormConfig();
					Client client = 
							NimbusClient.getConfiguredClient(conf).getClient();
					KillOptions killOpts = new KillOptions();
					killOpts.set_wait_secs(120); // time to wait before killing
					try {
						client.killTopologyWithOpts(RaceConfig.JstormTopologyName,
								killOpts);
					} catch (NotAliveException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (TException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					continue;
				}*/
				
			}else if( topic.equals(RaceConfig.MqPayTopic)) {
				PaymentMessage paymentMessage = RaceUtils.readKryoObject(
						PaymentMessage.class, body);
				collector.emit(new Values(topic,paymentMessage.getCreateTime()/ 1000/ 60,
						paymentMessage.getOrderId(),paymentMessage.getPayAmount(),
						paymentMessage.getPayPlatform()));
				//LOG.info("emit {}", paymentMessage);
			}
			else {
				
				OrderMessage orderMessage = RaceUtils.readKryoObject(
	        			OrderMessage.class, body);
				collector.emit(new Values(topic,orderMessage.getCreateTime()/ 1000/ 60,
						orderMessage.getOrderId(),0,0));
				//LOG.info("emit {}", orderMessage);
			}
		}
	}

	@Override
	public void nextTuple() {

		MetaTuple metaTuple = null;
		try {
			metaTuple = sendingQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (metaTuple == null) {
			return;
		}

		sendTuple(metaTuple);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic","createTime","orderID","payAmount","payPlatform"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
			ConsumeConcurrentlyContext context) {
		try {
			MetaTuple metaTuple = new MetaTuple(msgs, context.getMessageQueue());

			if (flowControl) {
				sendingQueue.offer(metaTuple);
			} else {
				sendTuple(metaTuple);
			}

			if (autoAck) {
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			} else {
				metaTuple.waitFinish();
				if (metaTuple.isSuccess() == true) {
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				} else {
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
			}

		} catch (Exception e) {
			LOG.error("Failed to emit " + id, e);
			return ConsumeConcurrentlyStatus.RECONSUME_LATER;
		}

	}

	public DefaultMQPushConsumer getConsumer() {
		return consumer;
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	/*@Override
	public void fail(Object msgId, List<Object> values) {
		MetaTuple metaTuple = (MetaTuple) values.get(0);
		AtomicInteger failTimes = metaTuple.getFailureTimes();

		int failNum = failTimes.incrementAndGet();
		if (failNum > metaClientConfig.getMaxFailTimes()) {
			LOG.warn("Message " + metaTuple.getMq() + " fail times " + failNum);
			finishTuple(metaTuple);
			return;
		}

		if (flowControl) {
			sendingQueue.offer(metaTuple);
		} else {
			sendTuple(metaTuple);
		}
	}

	public void finishTuple(MetaTuple metaTuple) {
		metaTuple.done();
	}

	@Override
	public void ack(Object msgId, List<Object> values) {
		MetaTuple metaTuple = (MetaTuple) values.get(0);
		finishTuple(metaTuple);
	}*/

}
