package com.mawen.learn.rocketmq.client.trace;

import com.mawen.learn.rocketmq.common.topic.TopicValidator;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
public class TraceConstants {

	public static final String GROUP_NAME_PREFIX = "_INNER_TRACE_RPODUCER";
	public static final char CONTENT_SPLITOR = (char) 1;
	public static final char FIELD_SPLITOR = (char) 2;
	public static final String TRACE_INSTANCE_NAME = "PID_CLIENT_INNER_TRACE_PRODUCER";
	public static final String TRACE_TOPIC_PREFIX = TopicValidator.SYSTEM_TOPIC_PREFIX + "TRACE_DATA_";
	public static final String TO_PREFIX = "To_";
	public static final String FROM_PREFIX = "From_";
	public static final String END_TRANSACTION = "EndTransaction";
	public static final String ROCKETMQ_SERVICE = "rocketmq";
	public static final String ROCKETMQ_SUCCESS = "rocketmq.success";
	public static final String ROCKETMQ_TAGS = "rocketmq.tags";
	public static final String ROCKETMQ_KEYS = "rocketmq.keys";
	public static final String ROCKETMQ_STORE_HOST = "rocketmq.store_host";
	public static final String ROCKETMQ_BODY_LENGTH = "rocketmq.body_length";
	public static final String ROCKETMQ_MSG_ID = "rocketmq.msg_id";
	public static final String ROCKETMQ_MSG_TYPE = "rocketmq.msg_type";
	public static final String ROCKETMQ_REGION_ID = "rocketmq.region_id";
	public static final String ROCKETMQ_TRANSACTION_ID = "rocketmq.transaction_id";
	public static final String ROCKETMQ_TRANSACTION_STATE = "rocketmq.transaction_state";
	public static final String ROCKETMQ_IS_FROM_TRANSACTION_CHECK = "rocketmq.is_from_transaction_check";
	public static final String ROCKETMQ_RETRY_TIMES = "rocketmq.retry_times";
}
