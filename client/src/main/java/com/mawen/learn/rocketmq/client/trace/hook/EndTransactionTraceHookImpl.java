package com.mawen.learn.rocketmq.client.trace.hook;

import java.util.Arrays;

import com.mawen.learn.rocketmq.client.hook.EndTransactionContext;
import com.mawen.learn.rocketmq.client.hook.EndTransactionHook;
import com.mawen.learn.rocketmq.client.trace.AsyncTraceDispatcher;
import com.mawen.learn.rocketmq.client.trace.TraceBean;
import com.mawen.learn.rocketmq.client.trace.TraceContext;
import com.mawen.learn.rocketmq.client.trace.TraceDispatcher;
import com.mawen.learn.rocketmq.client.trace.TraceType;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageType;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import lombok.AllArgsConstructor;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
@AllArgsConstructor
public class EndTransactionTraceHookImpl implements EndTransactionHook {

	private TraceDispatcher localDispatcher;

	@Override
	public String hookName() {
		return "EndTransactionTraceHook";
	}

	@Override
	public void endTransaction(EndTransactionContext context) {
		if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())) {
			return;
		}

		Message msg = context.getMessage();

		TraceContext traceContext = new TraceContext();
		traceContext.setTraceType(TraceType.EndTransaction);
		traceContext.setGroupName(NamespaceUtil.withoutNamespace(context.getProducerGroup()));

		TraceBean bean = new TraceBean();
		bean.setTopic(NamespaceUtil.withoutNamespace(context.getMessage().getTopic()));
		bean.setTags(context.getMessage().getTags());
		bean.setKeys(context.getMessage().getKeys());
		bean.setStoreHost(context.getBrokerAddr());
		bean.setMsgType(MessageType.Trans_Msg_Commit);
		bean.setClientHost(((AsyncTraceDispatcher) localDispatcher).getHostProducer().getMqClientFactory().getClientId());
		bean.setMsgId(context.getMsgId());
		bean.setTransactionState(context.getTransactionState());
		bean.setTransactionId(context.getTransactionId());
		bean.setFromTransactionCheck(context.isFromTransactionCheck());
		String regionId = msg.getProperty(MessageConst.PROPERTY_MSG_REGION);
		if (regionId == null || regionId.isEmpty()) {
			regionId = MixAll.DEFAULT_TRACE_REGION_ID;
		}

		traceContext.setRegionId(regionId);
		traceContext.setTraceBeans(Arrays.asList(bean));
		traceContext.setTimestamp(System.currentTimeMillis());

		localDispatcher.append(traceContext);
	}
}
