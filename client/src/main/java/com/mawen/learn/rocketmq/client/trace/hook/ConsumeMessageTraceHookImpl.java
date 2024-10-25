package com.mawen.learn.rocketmq.client.trace.hook;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.mawen.learn.rocketmq.client.consumer.listener.ConsumeReturnType;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageContext;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageHook;
import com.mawen.learn.rocketmq.client.trace.TraceBean;
import com.mawen.learn.rocketmq.client.trace.TraceContext;
import com.mawen.learn.rocketmq.client.trace.TraceDispatcher;
import com.mawen.learn.rocketmq.client.trace.TraceType;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Getter
@AllArgsConstructor
public class ConsumeMessageTraceHookImpl implements ConsumeMessageHook {

	private TraceDispatcher dispatcher;

	@Override
	public String hookName() {
		return "ConsumeMessageTraceHook";
	}

	@Override
	public void consumeMessageBefore(ConsumeMessageContext context) {
		if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
			return;
		}

		TraceContext traceContext = new TraceContext();
		context.setMqTraceContext(traceContext);
		traceContext.setTraceType(TraceType.SubBefore);
		traceContext.setGroupName(NamespaceUtil.withoutNamespace(context.getConsumerGroup()));

		List<TraceBean> beans = new ArrayList<>();
		for (MessageExt msg : context.getMsgList()) {
			if (msg == null) {
				continue;
			}

			String regionId = msg.getProperty(MessageConst.PROPERTY_MSG_REGION);
			String traceOn = msg.getProperty(MessageConst.PROPERTY_TRACE_SWITCH);
			if (traceOn != null && traceOn.equals("false")) {
				continue;
			}

			TraceBean traceBean = new TraceBean();
			traceBean.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic()));
			traceBean.setMsgId(msg.getMsgId());
			traceBean.setTags(msg.getTags());
			traceBean.setKeys(msg.getKeys());
			traceBean.setStoreTime(msg.getStoreTimestamp());
			traceBean.setBodyLength(msg.getStoreSize());
			traceBean.setRetryTimes(msg.getReconsumeTimes());
			beans.add(traceBean);

			traceContext.setRegionId(regionId);
		}

		if (beans.size() > 0) {
			traceContext.setTraceBeans(beans);
			traceContext.setTimestamp(System.currentTimeMillis());
			dispatcher.append(traceContext);
		}
	}

	@Override
	public void consumeMessageAfter(ConsumeMessageContext context) {
		if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
			return;
		}

		TraceContext subBeforeContext = (TraceContext) context.getMqTraceContext();
		if (CollectionUtils.isEmpty(subBeforeContext.getTraceBeans())) {
			return;
		}

		TraceContext subAfterContext = new TraceContext();
		subAfterContext.setTraceType(TraceType.SubAfter);
		subAfterContext.setRegionId(subBeforeContext.getRegionId());
		subAfterContext.setGroupName(NamespaceUtil.withoutNamespace(subBeforeContext.getGroupName()));
		subAfterContext.setRequestId(subBeforeContext.getRequestId());
		subAfterContext.setAccessChannel(subBeforeContext.getAccessChannel());
		subBeforeContext.setSuccess(subBeforeContext.isSuccess());

		int costTime = (int) ((System.currentTimeMillis() - subBeforeContext.getTimestamp()) / context.getMsgList().size());
		subAfterContext.setCostTime(costTime);
		subAfterContext.setTraceBeans(subBeforeContext.getTraceBeans());
		Map<String, String> props = context.getProps();
		if (props != null) {
			String contextType = props.get(MixAll.CONSUME_CONTEXT_TYPE);
			if (contextType != null) {
				subAfterContext.setContextCode(ConsumeReturnType.valueOf(contextType).ordinal());
			}
		}

		dispatcher.append(subAfterContext);
	}
}
