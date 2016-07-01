package com.alibaba.rocketmq.storm;


import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;

import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Von Gosling
 */
public class MessageConsumerManager {

    private static final Logger LOG = LoggerFactory.getLogger(MessageConsumerManager.class);

    MessageConsumerManager() {
    }

    public static MQConsumer getConsumerInstance(RocketMQConfig config, MessageListener listener,
                                                 Boolean isPushlet) throws MQClientException {
        LOG.info("Begin to init consumer,instanceName->{},configuration->{}",
                new Object[]{config.getInstanceName(), config});

        if (BooleanUtils.isTrue(isPushlet)) {
        	
        	 DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer();
             pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
             pushConsumer.setConsumerGroup(RaceConfig.MetaConsumerGroup + System.currentTimeMillis());
             pushConsumer.setNamesrvAddr("192.168.0.17:9876");
             pushConsumer.subscribe(RaceConfig.MqPayTopic, "*");
             pushConsumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
             pushConsumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
             
             
            /*DefaultMQPushConsumer pushConsumer = (DefaultMQPushConsumer) FastBeanUtils.copyProperties(config,
                    DefaultMQPushConsumer.class);
            pushConsumer.setConsumerGroup(config.getGroupId());
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            pushConsumer.setNamesrvAddr("192.168.0.17:9876");*/

            
            if (listener instanceof MessageListenerConcurrently) {
                pushConsumer.registerMessageListener((MessageListenerConcurrently) listener);
            }
            if (listener instanceof MessageListenerOrderly) {
                pushConsumer.registerMessageListener((MessageListenerOrderly) listener);
            }
            return pushConsumer;
        } else {
            DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer();
            pullConsumer.setConsumerGroup(config.getGroupId());

            return pullConsumer;
        }
    }
}
