package com.mawen.learn.rocketmq.remoting.protocol.subscription;

import java.util.Objects;

import com.google.common.base.MoreObjects;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class SimpleSubscriptionData {

	private String topic;

	private String expressionType;

	private String expression;

	private long version;

	public SimpleSubscriptionData(String topic, String expressionType, String expression, long version) {
		this.topic = topic;
		this.expressionType = expressionType;
		this.expression = expression;
		this.version = version;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getExpressionType() {
		return expressionType;
	}

	public void setExpressionType(String expressionType) {
		this.expressionType = expressionType;
	}

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SimpleSubscriptionData that = (SimpleSubscriptionData) o;
		return Objects.equals(topic, that.topic) && Objects.equals(expressionType, that.expressionType) && Objects.equals(expression, that.expression);
	}

	@Override
	public int hashCode() {
		return Objects.hash(topic, expressionType, expression);
	}

	@Override public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("topic", topic)
				.add("expressionType", expressionType)
				.add("expression", expression)
				.add("version", version)
				.toString();
	}
}
