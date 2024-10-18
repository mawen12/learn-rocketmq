package com.mawen.learn.rocketmq.client.trace;

import com.mawen.learn.rocketmq.client.producer.LocalTransactionState;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.message.MessageType;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
@Getter
@Setter
public class TraceBean {

	private static final String LOCAL_ADDRESS = UtilAll.ipToIPv4Str(UtilAll.getIP());

	private String topic = "";
	private String msgId = "";
	private String offsetMsgId = "";
	private String tags = "";
	private String keys = "";
	private String storeHost = LOCAL_ADDRESS;
	private String clientHost = LOCAL_ADDRESS;
	private long storeTime;
	private int retryTimes;
	private int bodyLength;
	private MessageType msgType;
	private LocalTransactionState transactionState;
	private String transactionId;
	private boolean fromTransactionCheck;
}
