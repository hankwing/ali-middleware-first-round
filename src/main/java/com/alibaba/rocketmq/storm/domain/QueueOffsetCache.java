package com.alibaba.rocketmq.storm.domain;

import java.util.Map;

import com.alibaba.rocketmq.common.message.MessageQueue;
import com.google.common.collect.Maps;

/**
 * @author Von Gosling
 */
public class QueueOffsetCache {

    private static final Map<MessageQueue, Long> offsetCache = Maps.newHashMap();

    public static void putMessageQueueOffset(MessageQueue mq, long offset) {
        offsetCache.put(mq, offset);
    }

    public static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = offsetCache.get(mq);
        if (offset != null)
            return offset;
        return 0;
    }

}
