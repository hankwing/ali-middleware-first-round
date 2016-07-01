package com.alibaba.middleware.components;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.tools.MetaTuple;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

public class TaobaoSpout implements IRichSpout, MessageListenerConcurrently {
	/**  */
	private static final long serialVersionUID = 8476906628618859716L;
	private static final Logger LOG = Logger.getLogger(TaobaoSpout.class);

	// protected DefaultMQPushConsumer metaClientConfig;
	protected SpoutOutputCollector collector;
	protected transient DefaultMQPushConsumer consumer;

	protected Map conf;
	protected String id;
	protected boolean flowControl = false;
	protected boolean autoAck = false;

	protected transient LinkedBlockingDeque<MetaTuple> sendingQueue;

	public TaobaoSpout() {

	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.conf = conf;
		this.collector = collector;
		this.sendingQueue = new LinkedBlockingDeque<MetaTuple>();

		// initMetricClient(context);

		try {
			consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
			consumer.setNamesrvAddr(RaceConfig.MqConfigServer);
			consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, null);
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
			consumer.registerMessageListener(this);
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
		collector.emit(new Values(metaTuple), metaTuple.getCreateMs());
	}

	@Override
	public void nextTuple() {
		MetaTuple metaTuple = null;
		try {
			metaTuple = sendingQueue.take();
		} catch (InterruptedException e) {
		}

		if (metaTuple == null) {
			return;
		}

		sendTuple(metaTuple);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MetaTuple"));
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

}
