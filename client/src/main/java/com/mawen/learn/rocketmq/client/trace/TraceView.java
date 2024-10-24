package com.mawen.learn.rocketmq.client.trace;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Getter
@Setter
public class TraceView {
	private String msgId;
	private String tags;
	private String keys;
	private String storeHost;
	private String clientHost;
	private int costTime;
	private String msgType;
	private String offsetMsgId;
	private long timestamp;
	private long bornTime;
	private String topic;
	private String groupName;
	private String status;

	public static List<TraceView> decodeFromTraceTransData(String key, MessageExt messageExt) throws UnsupportedEncodingException {
		List<TraceView> messageTraceViewList = new ArrayList<>();
		String messageBody = new String(messageExt.getBody(), MixAll.DEFAULT_CHARSET);
		if (StringUtils.isEmpty(messageBody)) {
			return messageTraceViewList;
		}

		List<TraceContext> traceContextList = TraceDataEncoder.decodeFromTraceDataString(messageBody);

		for (TraceContext context : traceContextList) {

			TraceBean traceBean = context.getTraceBeans().get(0);
			if (!traceBean.getMsgId().equals(key)) {
				continue;
			}

			TraceView messageTraceView = new TraceView();
			messageTraceView.setCostTime(context.getCostTime());
			messageTraceView.setGroupName(context.getGroupName());
			messageTraceView.setStatus(context.isSuccess() ? "success" : "failed");
			messageTraceView.setKeys(traceBean.getKeys());
			messageTraceView.setMsgId(traceBean.getMsgId());
			messageTraceView.setTopic(traceBean.getTopic());
			messageTraceView.setMsgType(context.getTraceType().name());
			messageTraceView.setOffsetMsgId(traceBean.getOffsetMsgId());
			messageTraceView.setTimestamp(context.getTimestamp());
			messageTraceView.setStoreHost(traceBean.getStoreHost());
			messageTraceView.setClientHost(messageExt.getBornHostString());

			messageTraceViewList.add(messageTraceView);
		}

		return messageTraceViewList;
	}
}
