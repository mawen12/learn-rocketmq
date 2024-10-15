package com.mawen.learn.rocketmq.remoting.metrics;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
public class RemotingMetricsConstant {

	public static final String HISTOGRAM_RPC_LATENCY = "rocketmq_rpc_latency";

	public static final String LABEL_PROTOCOL_TYPE = "protocol_type";

	public static final String LABEL_REQUEST_CODE = "request_code";

	public static final String LABEL_RESPONSE_CODE = "response_code";

	public static final String LABEL_IS_LONG_POLLING = "is_long_polling";

	public static final String LABEL_RESULT = "result";

	public static final String PROTOCOL_TYPE_REMOTING = "remoting";

	public static final String RESULT_ONEWAY = "oneway";

	public static final String RESULT_SUCCESS = "success";

	public static final String RESULT_CANCELED = "canceled";

	public static final String RESULT_PROCESS_REQUEST_FAILED = "process_result_failed";

	public static final String RESULT_WRITE_CHANNEL_FAILED = "write_channel_failed";
}
