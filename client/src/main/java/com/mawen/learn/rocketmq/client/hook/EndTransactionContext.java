package com.mawen.learn.rocketmq.client.hook;

import com.mawen.learn.rocketmq.client.producer.LocalTransactionState;
import com.mawen.learn.rocketmq.common.message.Message;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
@Getter
@Setter
public class EndTransactionContext {
	private String producerGroup;
	private Message message;
	private String brokerAddr;
	private String msgId;
	private String transactionId;
	private LocalTransactionState transactionState;
	private boolean fromTransactionCheck;
}
