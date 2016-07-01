package com.alibaba.rocketmq.storm.spout;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.RotatingMap;
import backtype.storm.utils.RotatingMap.ExpiredCallback;
import backtype.storm.utils.Utils;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.storm.domain.BatchMessage;
import com.alibaba.rocketmq.storm.domain.MessageCacheItem;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;
import com.google.common.collect.Sets;

/**
 * @author Von Gosling
 */
public class StreamMessageSpout extends BatchMessageSpout {
    private static final long                     serialVersionUID = 464153253576782163L;

    private static final Logger                   LOG              = LoggerFactory
                                                                           .getLogger(StreamMessageSpout.class);

    private final Queue<MessageCacheItem>         msgQueue         = new ConcurrentLinkedQueue<MessageCacheItem>();
    //private RotatingMap<String, MessageCacheItem> msgCache;

    /**
     * This field is used to check whether one batch is finish or not
     */
    //private Map<UUID, BatchMsgsTag>               batchMsgsMap     = new ConcurrentHashMap<UUID, BatchMsgsTag>();

    public StreamMessageSpout(RocketMQConfig config) {
		super(config);
		System.out.println("spout construct!");
		// TODO Auto-generated constructor stub
	}
    
    public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context,
                     final SpoutOutputCollector collector) {
        super.open(conf, context, collector);

        LOG.info("Topology {} opened {} spout successfully !",
                new Object[] { topologyName, config.getTopic() });

    }

    public void prepareMsg() {
        while (true) {
            BatchMessage msgTuple = null;
            try {
                msgTuple = super.getBatchQueue().poll(1, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return;
            }
            if (msgTuple == null) {
                return;
            }

            if (msgTuple.getMsgList().size() == 0) {
                super.finish(msgTuple.getBatchId());
                return;
            }

            BatchMsgsTag partTag = new BatchMsgsTag();
            Set<String> msgIds = partTag.getMsgIds();
            for (MessageExt msg : msgTuple.getMsgList()) {
                String msgId = msg.getMsgId();
                msgIds.add(msgId);
                MessageCacheItem item = new MessageCacheItem(msgTuple.getBatchId(), msg,
                        msgTuple.getMessageStat());
                msgQueue.offer(item);
            }
        }
    }

    @Override
    public void nextTuple() {
        MessageCacheItem cacheItem = msgQueue.poll();
        if(cacheItem!=null && cacheItem.isStop()) {
        	// receive stop sign
        	LOG.info("stop the topology!!!");
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
        	
        }
        else if(cacheItem != null){          
            Values values = new Values(cacheItem.getTopic(), cacheItem.getCreateTime(), 
            		cacheItem.getOrderId(), cacheItem.getPayAmount(), cacheItem.getPayPlatform());
            String messageId = cacheItem.getMsg().getMsgId();
            collector.emit(values,messageId );
            //LOG.info("Emited tuple {},mssageId is {} !", values, messageId);
            return;
        }

        prepareMsg();

        return;
    }


    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("topic","createTime","orderID","payAmount","payPlatform"));
    }

    public static class BatchMsgsTag {
        private final Set<String> msgIds;
        private final long        createTs;

        public BatchMsgsTag() {
            this.msgIds = Sets.newHashSet();
            this.createTs = System.currentTimeMillis();
        }

        public Set<String> getMsgIds() {
            return msgIds;
        }

        public long getCreateTs() {
            return createTs;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

}
