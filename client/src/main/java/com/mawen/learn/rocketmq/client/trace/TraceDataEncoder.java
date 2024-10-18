package com.mawen.learn.rocketmq.client.trace;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mawen.learn.rocketmq.client.AccessChannel;
import com.mawen.learn.rocketmq.client.producer.LocalTransactionState;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageType;

import static com.mawen.learn.rocketmq.client.trace.TraceConstants.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
public class TraceDataEncoder {

	public static List<TraceContext> decodeFromTraceDataString(String traceData) {
		List<TraceContext> resList = new ArrayList<>();
		if (traceData == null || traceData.length() < 0) {
			return resList;
		}

		String[] contextList = traceData.split(String.valueOf(FIELD_SPLITOR));
		for (String context : contextList) {
			String[] line = context.split(String.valueOf(CONTENT_SPLITOR));
			if (line[0].equals(TraceType.Pub.name())) {
				TraceContext pubContext = new TraceContext();
				pubContext.setTraceType(TraceType.Pub);
				pubContext.setTimestamp(Long.parseLong(line[1]));
				pubContext.setRegionId(line[2]);
				pubContext.setGroupName(line[3]);

				TraceBean bean = new TraceBean();
				bean.setTopic(line[4]);
				bean.setMsgId(line[5]);
				bean.setTags(line[6]);
				bean.setKeys(line[7]);
				bean.setStoreHost(line[8]);
				bean.setBodyLength(Integer.parseInt(line[9]));

				pubContext.setCostTime(Integer.parseInt(line[10]));
				bean.setMsgType(MessageType.values()[Integer.parseInt(line[11])]);

				if (line.length == 13) {
					pubContext.setSuccess(Boolean.parseBoolean(line[12]));
				}
				else if (line.length == 14) {
					bean.setOffsetMsgId(line[12]);
					pubContext.setSuccess(Boolean.parseBoolean(line[13]));
				}

				if (line.length >= 15) {
					bean.setOffsetMsgId(line[12]);
					pubContext.setSuccess(Boolean.parseBoolean(line[13]));
					bean.setClientHost(line[14]);
				}

				pubContext.setTraceBeans(new ArrayList<>(1));
				pubContext.getTraceBeans().add(bean);
				resList.add(pubContext);
			}
			else if (line[0].equals(TraceType.SubBefore.name())) {
				TraceContext subBeforeContext = new TraceContext();
				subBeforeContext.setTraceType(TraceType.SubBefore);
				subBeforeContext.setTimestamp(Long.parseLong(line[1]));
				subBeforeContext.setRegionId(line[2]);
				subBeforeContext.setGroupName(line[3]);
				subBeforeContext.setRequestId(line[4]);

				TraceBean bean = new TraceBean();
				bean.setMsgId(line[5]);
				bean.setRetryTimes(Integer.parseInt(line[6]));
				bean.setKeys(line[7]);

				subBeforeContext.setTraceBeans(Arrays.asList(bean));
				resList.add(subBeforeContext);
			}
			else if (line[0].equals(TraceType.SubAfter.name())) {
				TraceContext subAfterContext = new TraceContext();
				subAfterContext.setTraceType(TraceType.SubAfter);
				subAfterContext.setRequestId(line[1]);

				TraceBean bean = new TraceBean();
				bean.setMsgId(line[2]);
				bean.setKeys(line[5]);

				subAfterContext.setTraceBeans(Arrays.asList(bean));
				subAfterContext.setCostTime(Integer.parseInt(line[3]));
				subAfterContext.setSuccess(Boolean.parseBoolean(line[4]));

				if (line.length >= 7) {
					subAfterContext.setContextCode(Integer.parseInt(line[6]));
				}
				if (line.length >= 9) {
					subAfterContext.setTimestamp(Long.parseLong(line[7]));
					subAfterContext.setGroupName(line[8]);
				}
				resList.add(subAfterContext);
			}
			else if (line[0].equals(TraceType.EndTransaction.name())) {
				TraceContext endTransactionContext = new TraceContext();
				endTransactionContext.setTraceType(TraceType.EndTransaction);
				endTransactionContext.setTimestamp(Long.parseLong(line[1]));
				endTransactionContext.setRegionId(line[2]);
				endTransactionContext.setGroupName(line[3]);

				TraceBean bean = new TraceBean();
				bean.setTopic(line[4]);
				bean.setMsgId(line[5]);
				bean.setTags(line[6]);
				bean.setKeys(line[7]);
				bean.setStoreHost(line[8]);
				bean.setMsgType(MessageType.values()[Integer.parseInt(line[9])]);
				bean.setTransactionId(line[10]);
				bean.setTransactionState(LocalTransactionState.valueOf(line[11]));
				bean.setFromTransactionCheck(Boolean.parseBoolean(line[12]));

				endTransactionContext.setTraceBeans(Arrays.asList(bean));
				resList.add(endTransactionContext);
			}
		}
		return resList;
	}

	public static TraceTransferBean encodeFromContextBean(TraceContext context) {
		if (context == null) {
			return null;
		}

		TraceTransferBean traceTransferBean = new TraceTransferBean();
		StringBuilder sb = new StringBuilder(256);

		switch (context.getTraceType()) {
			case Pub: {
				TraceBean bean = context.getTraceBeans().get(0);
				sb.append(context.getTraceType()).append(CONTENT_SPLITOR)
						.append(context.getTimestamp()).append(CONTENT_SPLITOR)
						.append(context.getRegionId()).append(CONTENT_SPLITOR)
						.append(context.getGroupName()).append(CONTENT_SPLITOR)
						.append(bean.getTopic()).append(CONTENT_SPLITOR)
						.append(bean.getMsgId()).append(CONTENT_SPLITOR)
						.append(bean.getKeys()).append(CONTENT_SPLITOR)
						.append(bean.getStoreHost()).append(CONTENT_SPLITOR)
						.append(bean.getBodyLength()).append(CONTENT_SPLITOR)
						.append(context.getCostTime()).append(CONTENT_SPLITOR)
						.append(bean.getMsgType().ordinal()).append(CONTENT_SPLITOR)
						.append(bean.getOffsetMsgId()).append(CONTENT_SPLITOR)
						.append(context.isSuccess()).append(FIELD_SPLITOR);
				break;
			}
			case SubBefore: {
				for (TraceBean bean : context.getTraceBeans()) {
					sb.append(context.getTraceType()).append(CONTENT_SPLITOR)
							.append(context.getTimestamp()).append(CONTENT_SPLITOR)
							.append(context.getRegionId()).append(CONTENT_SPLITOR)
							.append(context.getGroupName()).append(CONTENT_SPLITOR)
							.append(context.getRequestId()).append(CONTENT_SPLITOR)
							.append(bean.getMsgType()).append(CONTENT_SPLITOR)
							.append(bean.getRetryTimes()).append(CONTENT_SPLITOR)
							.append(bean.getKeys()).append(FIELD_SPLITOR);
				}
				break;
			}
			case SubAfter: {
				for (TraceBean bean : context.getTraceBeans()) {
					sb.append(context.getTraceType()).append(CONTENT_SPLITOR)
							.append(context.getRequestId()).append(CONTENT_SPLITOR)
							.append(bean.getMsgId()).append(CONTENT_SPLITOR)
							.append(context.getCostTime()).append(CONTENT_SPLITOR)
							.append(context.isSuccess()).append(CONTENT_SPLITOR)
							.append(bean.getKeys()).append(CONTENT_SPLITOR)
							.append(context.getContextCode()).append(CONTENT_SPLITOR);

					if (!context.getAccessChannel().equals(AccessChannel.CLOUD)) {
						sb.append(context.getTimestamp()).append(CONTENT_SPLITOR)
								.append(context.getGroupName()).append(FIELD_SPLITOR);
					}
				}
				break;
			}
			case EndTransaction: {
				TraceBean bean = context.getTraceBeans().get(0);
				sb.append(context.getTraceType()).append(CONTENT_SPLITOR)
						.append(context.getTimestamp()).append(CONTENT_SPLITOR)
						.append(context.getRegionId()).append(CONTENT_SPLITOR)
						.append(context.getGroupName()).append(CONTENT_SPLITOR)
						.append(bean.getTopic()).append(CONTENT_SPLITOR)
						.append(bean.getMsgId()).append(CONTENT_SPLITOR)
						.append(bean.getTags()).append(CONTENT_SPLITOR)
						.append(bean.getKeys()).append(CONTENT_SPLITOR)
						.append(bean.getStoreHost()).append(CONTENT_SPLITOR)
						.append(bean.getMsgType()).append(CONTENT_SPLITOR)
						.append(bean.getTransactionId()).append(CONTENT_SPLITOR)
						.append(bean.getTransactionState().name()).append(CONTENT_SPLITOR)
						.append(bean.isFromTransactionCheck()).append(FIELD_SPLITOR);
				break;
			}
			default:
				break;
		}

		traceTransferBean.setTransData(sb.toString());

		for (TraceBean bean : context.getTraceBeans()) {
			traceTransferBean.getTransKey().add(bean.getMsgId());

			if (bean.getKeys() != null && bean.getKeys().length() > 0) {
				String[] keys = bean.getKeys().split(MessageConst.KEY_SEPARATOR);
				traceTransferBean.getTransKey().addAll(Arrays.asList(keys));
			}
		}

		return traceTransferBean;
	}
}
