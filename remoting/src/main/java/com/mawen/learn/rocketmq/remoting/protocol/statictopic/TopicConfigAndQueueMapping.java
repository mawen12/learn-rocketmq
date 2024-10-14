package com.mawen.learn.rocketmq.remoting.protocol.statictopic;

import com.mawen.learn.rocketmq.common.TopicConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class TopicConfigAndQueueMapping extends TopicConfig {

	private TopicQueueMappingDetail mappingDetail;

	public TopicConfigAndQueueMapping() {
	}

	public TopicConfigAndQueueMapping(TopicConfig topicConfig, TopicQueueMappingDetail mappingDetail) {
		super(topicConfig);
		this.mappingDetail = mappingDetail;
	}

	public TopicQueueMappingDetail getMappingDetail() {
		return mappingDetail;
	}

	public void setMappingDetail(TopicQueueMappingDetail mappingDetail) {
		this.mappingDetail = mappingDetail;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;

		if (!(o instanceof TopicConfigAndQueueMapping)) return false;

		TopicConfigAndQueueMapping that = (TopicConfigAndQueueMapping) o;

		return new EqualsBuilder()
				.appendSuper(super.equals(o))
				.append(mappingDetail, that.mappingDetail)
				.isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37)
				.appendSuper(super.hashCode())
				.append(mappingDetail)
				.toHashCode();
	}

	@Override
	public String toString() {
		String string = super.toString();
		if (StringUtils.isNotBlank(string)) {
			string = string.substring(0, string.length() - 1) + ", mappingDetail=" + mappingDetail + "]";
		}
		return string;
	}
}
