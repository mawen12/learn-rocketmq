package com.mawen.learn.rocketmq.remoting.protocol.subscription;

import com.alibaba.fastjson2.annotation.JSONField;
import com.google.common.base.MoreObjects;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class GroupRetryPolicy {

	private static final RetryPolicy DEFAULT_RETRY_POLICY = new CustomizedRetryPolicy();

	private GroupRetryPolicyType type = GroupRetryPolicyType.CUSTOMIZED;

	private ExponentialRetryPolicy exponentialRetryPolicy;

	private CustomizedRetryPolicy customizedRetryPolicy;

	@JSONField(serialize = false, deserialize = false)
	public RetryPolicy getRetryPolicy() {
		if (GroupRetryPolicyType.EXPONENTIAL.equals(type)) {
			if (exponentialRetryPolicy == null) {
				return DEFAULT_RETRY_POLICY;
			}
			return exponentialRetryPolicy;
		}
		else if (GroupRetryPolicyType.CUSTOMIZED.equals(type)) {
			if (customizedRetryPolicy == null) {
				return DEFAULT_RETRY_POLICY;
			}
			return customizedRetryPolicy;
		}
		else {
			return DEFAULT_RETRY_POLICY;
		}
	}

	public GroupRetryPolicyType getType() {
		return type;
	}

	public void setType(GroupRetryPolicyType type) {
		this.type = type;
	}

	public ExponentialRetryPolicy getExponentialRetryPolicy() {
		return exponentialRetryPolicy;
	}

	public void setExponentialRetryPolicy(ExponentialRetryPolicy exponentialRetryPolicy) {
		this.exponentialRetryPolicy = exponentialRetryPolicy;
	}

	public CustomizedRetryPolicy getCustomizedRetryPolicy() {
		return customizedRetryPolicy;
	}

	public void setCustomizedRetryPolicy(CustomizedRetryPolicy customizedRetryPolicy) {
		this.customizedRetryPolicy = customizedRetryPolicy;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("type", type)
				.add("exponentialRetryPolicy", exponentialRetryPolicy)
				.add("customizedRetryPolicy", customizedRetryPolicy)
				.toString();
	}
}
