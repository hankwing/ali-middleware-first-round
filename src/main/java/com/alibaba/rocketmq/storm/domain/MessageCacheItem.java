package com.alibaba.rocketmq.storm.domain;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.util.UUID;

/**
 * @author Von Gosling
 */
public class MessageCacheItem {
	
    private final UUID        id;
    private final MessageExt  msg;
    private final MessageStat msgStat;
    private final String topic;
    private Long createTime = 0L;
    private Long orderID = 0L;
    private double payAmount = 0;
    private short payPlatform = 0;
    private boolean isStop = false;

    public MessageCacheItem(UUID id, MessageExt msg, MessageStat msgStat) {
        this.id = id;
        this.msg = msg;
        this.msgStat = msgStat;
        this.topic = msg.getTopic();
       // System.out.println("receive tuple:" + topic);
        byte[] body = msg.getBody();
		if (body.length == 2 && body[0] == 0
				&& body[1] == 0) {
			System.out.println("spout to end!!!!");
			isStop = true;
		}
		
        if( topic.equals(RaceConfig.MqPayTopic)) {
        	PaymentMessage paymentMessage = RaceUtils.readKryoObject(
    						PaymentMessage.class, body);
        	createTime = paymentMessage.getCreateTime() / 1000 / 60; 	// in seconds
        	orderID = paymentMessage.getOrderId();
        	payAmount = paymentMessage.getPayAmount();
        	payPlatform = paymentMessage.getPayPlatform();
        	
        }
        else if (topic.equals(RaceConfig.MqTaobaoTradeTopic)) {
        	OrderMessage taobaoMessage = RaceUtils.readKryoObject(
        			OrderMessage.class, body);
        	createTime = taobaoMessage.getCreateTime() / 1000 / 60;
        	orderID = taobaoMessage.getOrderId();
        }
        else if ( topic.equals(RaceConfig.MqTmallTradeTopic)) {
        	OrderMessage tmallMessage = RaceUtils.readKryoObject(
        			OrderMessage.class, body);
        	createTime = tmallMessage.getCreateTime() / 1000 / 60;
        	orderID = tmallMessage.getOrderId();
        }
    }

    public UUID getId() {
        return id;
    }
    
    public boolean isStop() {
    	return isStop;
    }

    public MessageExt getMsg() {
        return msg;
    }

    public MessageStat getMsgStat() {
        return msgStat;
    }
    
    public Long getCreateTime() {
    	return createTime;
    }
    
    public Long getOrderId() {
    	return orderID;
    }
    
    public double getPayAmount() {
    	return payAmount;
    }
    
    public short getPayPlatform() {
    	return payPlatform;
    }
    
    public String getTopic() {
    	return topic;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
