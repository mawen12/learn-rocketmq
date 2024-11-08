package com.mawen.learn.rocketmq.store.hook;

import java.util.List;

import com.mawen.learn.rocketmq.common.message.MessageExt;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/8
 */
public interface SendMessageBackHook {

	boolean executeSendMessageBack(List<MessageExt> msgList, String brokerName, String brokerAddr);
}
