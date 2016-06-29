package com.alibaba.middleware.components;

import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class PaySpout implements IRichSpout {
	private static Logger LOG = LoggerFactory.getLogger(PaySpout.class);
	SpoutOutputCollector _collector;
	DefaultMQPullConsumer consumer;
	Set<MessageQueue> mqs = null;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;

		// 在本地搭建好broker后,记得指定nameServer的地址
		consumer = new DefaultMQPullConsumer("Payment");
		consumer.setNamesrvAddr(RaceConfig.MqConfigServer);
		try {
			consumer.start();
			mqs = consumer.fetchSubscribeMessageQueues(RaceConfig.MqPayTopic);
			for (MessageQueue mq : mqs) {
				consumer.updateConsumeOffset(mq, 0);
			}
			consumer.registerMessageQueueListener(RaceConfig.MqPayTopic, null);
		} catch (MQClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void nextTuple() {
		for (MessageQueue mq : mqs) {
			/*System.out.println("Consume message from queue: " + mq + " mqsize="
					+ mqs.size());*/
			int cnter = 0;
			// 每个队列里无限循环，分批拉取未消费的消息，直到拉取不到新消息为止
			SINGLE_MQ: while (cnter ++ < RaceConfig.emit_One_Time) {
				long offset;
				try {
					offset = consumer.fetchConsumeOffset(mq, false);
					offset = offset < 0 ? 0 : offset;
					//System.out.println("消费进度 Offset: " + offset);
					PullResult result = consumer.pull(mq, null, offset, 10);
					//System.out.println("接收到的消息集合" + result);

					switch (result.getPullStatus()) {
					case FOUND:
						if (result.getMsgFoundList() != null) {
							int prSize = result.getMsgFoundList().size();
							/*System.out
									.println("pullResult.getMsgFoundList().size()===="
											+ prSize);*/
							if (prSize != 0) {
								for (MessageExt me : result.getMsgFoundList()) {
									// 消费每条消息，如果消费失败，比如更新数据库失败，就重新再拉一次消息

									byte[] body = me.getBody();
									if (body.length == 2 && body[0] == 0
											&& body[1] == 0) {
										// Info: 生产者停止生成数据, 并不意味着马上结束
										System.out.println("spout to end!!!!");
										/*Map conf = Utils.readStormConfig();
										Client client = 
												NimbusClient.getConfiguredClient(conf).getClient();
										KillOptions killOpts = new KillOptions();
										killOpts.set_wait_secs(120); // time to wait before killing
										client.killTopologyWithOpts(RaceConfig.JstormTopologyName,
												killOpts);*/
										continue;
									}
									PaymentMessage paymentMessage = RaceUtils
											.readKryoObject(
													PaymentMessage.class, body);
									_collector.emit(new Values(paymentMessage.getCreateTime()/ 1000/ 60,
											paymentMessage.getOrderId(),paymentMessage.getPayAmount(),
											paymentMessage.getPayPlatform()));
									//System.out.println(paymentMessage);
								}

							}
						}
						// 获取下一个下标位置
						offset = result.getNextBeginOffset();
						// 消费完后，更新消费进度
						consumer.updateConsumeOffset(mq, offset);
						break;
					case NO_MATCHED_MSG:
						System.out.println("没有匹配的消息");
						break;
					case NO_NEW_MSG:
						System.out.println("没有未消费的新消息");
						// 拉取不到新消息，跳出 SINGLE_MQ 当前队列循环，开始下一队列循环。
						break SINGLE_MQ;
					case OFFSET_ILLEGAL:
						System.out.println("下标错误");
						break;
					default:
						break;
					}
				} catch (MQClientException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (RemotingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (MQBrokerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} /*catch (NotAliveException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/

			}
		}
	}

	@Override
	public void ack(Object id) {
		// Ignored
	}

	@Override
	public void fail(Object id) {
		_collector.emit(new Values(id), id);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("createTime","orderID","payAmount","payPlatform"));
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}
  
	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}