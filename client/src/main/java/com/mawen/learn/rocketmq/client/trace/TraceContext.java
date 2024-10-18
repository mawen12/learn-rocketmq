package com.mawen.learn.rocketmq.client.trace;

import java.util.List;

import com.mawen.learn.rocketmq.client.AccessChannel;
import com.mawen.learn.rocketmq.common.message.MessageClientIDSetter;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
@Setter
@Getter
public class TraceContext implements Comparable<TraceContext> {

	private TraceType traceType;
	private long timestamp = System.currentTimeMillis();
	private String regionId = "";
	private String regionName = "";
	private String groupName = "";
	private int costTime = 0;
	private boolean isSuccess = true;
	private String requestId = MessageClientIDSetter.createUniqID();
	private int contextCode = 0;
	private AccessChannel accessChannel;
	private List<TraceBean> traceBeans;

	@Override
	public int compareTo(TraceContext o) {
		return Long.compare(this.timestamp, o.getTimestamp());
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(1024);
		sb.append("TraceContext{").append(traceType).append("_")
				.append(groupName).append("_")
				.append(regionId).append("_")
				.append(isSuccess).append("_");

		if (traceBeans != null && traceBeans.size() > 0) {
			for (TraceBean bean : traceBeans) {
				sb.append(bean.getMsgId()).append("_").append(bean.getTopic()).append("_");
			}
		}

		sb.append('}');
		return sb.toString();
	}
}
