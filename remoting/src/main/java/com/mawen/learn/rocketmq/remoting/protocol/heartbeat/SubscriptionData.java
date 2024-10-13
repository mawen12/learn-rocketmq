package com.mawen.learn.rocketmq.remoting.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;

import com.alibaba.fastjson2.annotation.JSONField;
import com.mawen.learn.rocketmq.common.filter.ExpressionType;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class SubscriptionData implements Comparable<SubscriptionData> {

	public static final String SUB_ALL = "*";

	private boolean classFilterMode = false;

	private String topic;

	private String subString;

	private Set<String> tagsSet = new HashSet<>();

	private Set<Integer> codeSet = new HashSet<>();

	private long subVersion = System.currentTimeMillis();

	private String expressionType = ExpressionType.TAG;

	@JSONField(serialize = false)
	private String filterClassSource;

	public SubscriptionData() {
	}

	public SubscriptionData(String topic, String subString) {
		this.topic = topic;
		this.subString = subString;
	}

	public boolean isClassFilterMode() {
		return classFilterMode;
	}

	public void setClassFilterMode(boolean classFilterMode) {
		this.classFilterMode = classFilterMode;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getSubString() {
		return subString;
	}

	public void setSubString(String subString) {
		this.subString = subString;
	}

	public Set<String> getTagsSet() {
		return tagsSet;
	}

	public void setTagsSet(Set<String> tagsSet) {
		this.tagsSet = tagsSet;
	}

	public Set<Integer> getCodeSet() {
		return codeSet;
	}

	public void setCodeSet(Set<Integer> codeSet) {
		this.codeSet = codeSet;
	}

	public long getSubVersion() {
		return subVersion;
	}

	public void setSubVersion(long subVersion) {
		this.subVersion = subVersion;
	}

	public String getExpressionType() {
		return expressionType;
	}

	public void setExpressionType(String expressionType) {
		this.expressionType = expressionType;
	}

	public String getFilterClassSource() {
		return filterClassSource;
	}

	public void setFilterClassSource(String filterClassSource) {
		this.filterClassSource = filterClassSource;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (classFilterMode ? 1231 : 1237);
		result = prime * result + ((codeSet == null) ? 0 : codeSet.hashCode());
		result = prime * result + ((subString == null) ? 0 : subString.hashCode());
		result = prime * result + ((tagsSet == null) ? 0 : tagsSet.hashCode());
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		result = prime * result + ((expressionType == null) ? 0 : expressionType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SubscriptionData other = (SubscriptionData) obj;
		if (classFilterMode != other.classFilterMode)
			return false;
		if (codeSet == null) {
			if (other.codeSet != null)
				return false;
		} else if (!codeSet.equals(other.codeSet))
			return false;
		if (subString == null) {
			if (other.subString != null)
				return false;
		} else if (!subString.equals(other.subString))
			return false;
		if (subVersion != other.subVersion)
			return false;
		if (tagsSet == null) {
			if (other.tagsSet != null)
				return false;
		} else if (!tagsSet.equals(other.tagsSet))
			return false;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		if (expressionType == null) {
			if (other.expressionType != null)
				return false;
		} else if (!expressionType.equals(other.expressionType))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SubscriptionData [classFilterMode=" + classFilterMode + ", topic=" + topic + ", subString="
		       + subString + ", tagsSet=" + tagsSet + ", codeSet=" + codeSet + ", subVersion=" + subVersion
		       + ", expressionType=" + expressionType + "]";
	}

	@Override
	public int compareTo(SubscriptionData other) {
		String thisValue = this.topic + "@" + this.subString;
		String otherValue = other.topic + "@" + other.subString;
		return thisValue.compareTo(otherValue);
	}
}
