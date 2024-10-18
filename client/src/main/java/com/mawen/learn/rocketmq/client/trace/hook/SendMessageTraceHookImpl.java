package com.mawen.learn.rocketmq.client.trace.hook;

import java.util.ArrayList;
import java.util.Arrays;

import com.mawen.learn.rocketmq.client.hook.SendMessageContext;
import com.mawen.learn.rocketmq.client.hook.SendMessageHook;
import com.mawen.learn.rocketmq.client.producer.SendStatus;
import com.mawen.learn.rocketmq.client.trace.AsyncTraceDispatcher;
import com.mawen.learn.rocketmq.client.trace.TraceBean;
import com.mawen.learn.rocketmq.client.trace.TraceContext;
import com.mawen.learn.rocketmq.client.trace.TraceDispatcher;
import com.mawen.learn.rocketmq.client.trace.TraceType;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import lombok.AllArgsConstructor;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
@AllArgsConstructor
public class SendMessageTraceHookImpl implements SendMessageHook {

	private TraceDispatcher localDispatcher;

	@Override
	public String hookName() {
		return "SendMessageTraceHook";
	}

	@Override
	public void sendMessageBefore(SendMessageContext context) {
		if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())) {
			return;
		}

		TraceContext traceContext = new TraceContext();
		traceContext.setTraceType(TraceType.Pub);
		traceContext.setGroupName(NamespaceUtil.withoutNamespace(context.getProducerGroup()));

		TraceBean bean = new TraceBean();
		bean.setTopic(NamespaceUtil.withoutNamespace(context.getMessage().getTopic()));
		bean.setTags(context.getMessage().getTags());
		bean.setKeys(context.getMessage().getKeys());
		bean.setStoreHost(context.getBrokerAddr());
		bean.setBodyLength(context.getMessage().getBody().length);
		bean.setMsgType(context.getMsgType());

		traceContext.setTraceBeans(Arrays.asList(bean));
	}

	@Override
	public void sendMessageAfter(SendMessageContext context) {
		if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName()) || context.getMqTraceContext() == null) {
			return;
		}

		if (context.getSendResult() == null) {
			return;
		}

		if (context.getSendResult().getRegionId() == null || !context.getSendResult().isTraceOn()) {
			return;
		}

		TraceContext traceContext = (TraceContext) context.getMqTraceContext();
		TraceBean bean = traceContext.getTraceBeans().get(0);

		int costTime = (int) ((System.currentTimeMillis() - traceContext.getTimestamp()) / traceContext.getTraceBeans().size());
		traceContext.setCostTime(costTime);

		if (context.getSendResult().getSendStatus().equals(SendStatus.SEND_OK)) {
			traceContext.setSuccess(true);
		}
		else {
			traceContext.setSuccess(false);
		}

		traceContext.setRegionId(context.getSendResult().getRegionId());

		bean.setMsgId(context.getSendResult().getMsgId());
		bean.setOffsetMsgId(context.getSendResult().getOffsetMsgId());
		bean.setStoreTime(traceContext.getTimestamp() + costTime / 2);

		localDispatcher.append(traceContext);
	}
}
