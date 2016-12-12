package com.alibaba.middleware.components;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Map;

/**
 * @author Von Gosling
 */
public class DSBolt implements IBasicBolt {
    private static final long   serialVersionUID = 7591260982890048043L;

    private static final Logger LOG = LoggerFactory.getLogger(DSBolt.class);

    private OutputCollector     collector;

    @Override
    public void cleanup() {
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
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String topic = input.getString(0);
		List<MessageExt> meList = (List<MessageExt>) input.getValue(1);
		byte[] body = null;
		for( MessageExt message: meList) {
			body = message.getBody();
			if ( body.length ==2 && body[0] == 0) {
				// Info: 生产者停止生成数据, 并不意味着马上结束
				LOG.info("receive stop signs:{}", body );
				
			}else if( topic.equals(RaceConfig.MqPayTopic)) {
				PaymentMessage paymentMessage = RaceUtils.readKryoObject(
						PaymentMessage.class, body);
				collector.emit(new Values(topic,paymentMessage.getCreateTime(),
						paymentMessage.getOrderId(),paymentMessage.getPayAmount(),
						paymentMessage.getPayPlatform()));
				//LOG.info("emit {}", paymentMessage);
			}
			else {
				
				OrderMessage orderMessage = RaceUtils.readKryoObject(
	        			OrderMessage.class, body);
				collector.emit(new Values(topic,orderMessage.getCreateTime(),
						orderMessage.getOrderId(),orderMessage.getTotalPrice(),0));
				//LOG.info("emit {}", orderMessage);
			}
		}
		
	}
}
