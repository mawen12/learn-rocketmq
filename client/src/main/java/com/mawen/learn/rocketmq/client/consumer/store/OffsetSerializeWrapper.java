package com.mawen.learn.rocketmq.client.consumer.store;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
@Getter
@Setter
public class OffsetSerializeWrapper extends RemotingSerializable {

	private ConcurrentMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<>();

}
