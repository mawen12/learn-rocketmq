package com.mawen.learn.rocketmq.remoting.protocol;

import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class NamespaceUtil {
	public static final char NAMESPACE_SEPARATOR = '%';
	public static final String STRING_BLANK = "";
	public static final int RETRY_PREFIX_LENGTH = MixAll.RETRY_GROUP_TOPIC_PREFIX.length();
	public static final int DLQ_PREFIX_LENGTH = MixAll.DLQ_GROUP_TOPIC_PREFIX.length();

	public static String withoutNamespace(String resourceWithNameSpace) {
		if (StringUtils.isEmpty(resourceWithNameSpace) || isSystemResource(resourceWithNameSpace)) {
			return resourceWithNameSpace;
		}

		StringBuilder sb = new StringBuilder();
		if (isRetryTopic(resourceWithNameSpace)) {
			sb.append(MixAll.RETRY_GROUP_TOPIC_PREFIX);
		}

		if (isDLQTopic(resourceWithNameSpace)) {
			sb.append(MixAll.DLQ_GROUP_TOPIC_PREFIX);
		}

		String resourceWithoutRetryAndDLQ = withoutRetryAndDLQ(resourceWithNameSpace);
		int index = resourceWithoutRetryAndDLQ.indexOf(NAMESPACE_SEPARATOR);
		if (index > 0) {
			String resourceWithoutNamespace = resourceWithoutRetryAndDLQ.substring(index + 1);
			return sb.append(resourceWithoutNamespace).toString();
		}
		return resourceWithNameSpace;
	}

	public static String withoutNamespace(String resourceWithNamespace, String namespace) {
		if (StringUtils.isEmpty(resourceWithNamespace) || StringUtils.isEmpty(namespace)) {
			return resourceWithNamespace;
		}

		String resourceWithoutRetryAndDLQ = withoutRetryAndDLQ(resourceWithNamespace);
		if (resourceWithoutRetryAndDLQ.startsWith(namespace + NAMESPACE_SEPARATOR)) {
			return withoutNamespace(resourceWithNamespace);
		}
		return resourceWithNamespace;
	}

	public static String wrapNamespace(String namespace, String resourceWithoutNamespace) {
		if (StringUtils.isEmpty(namespace) || StringUtils.isEmpty(resourceWithoutNamespace)) {
			return resourceWithoutNamespace;
		}

		if (isSystemResource(resourceWithoutNamespace) || isAlreadyWithNamespace(resourceWithoutNamespace, namespace)) {
			return resourceWithoutNamespace;
		}

		String resourceWithoutRetryAndDLQ = withoutRetryAndDLQ(resourceWithoutNamespace);
		StringBuilder sb = new StringBuilder();

		if (isRetryTopic(resourceWithoutNamespace)) {
			sb.append(MixAll.RETRY_GROUP_TOPIC_PREFIX);
		}

		if (isDLQTopic(resourceWithoutNamespace)) {
			sb.append(MixAll.DLQ_GROUP_TOPIC_PREFIX);
		}

		return sb.append(namespace)
				.append(NAMESPACE_SEPARATOR)
				.append(resourceWithoutRetryAndDLQ)
				.toString();
	}

	public static boolean isAlreadyWithNamespace(String resource, String namespace) {
		if (StringUtils.isEmpty(namespace) || StringUtils.isEmpty(resource) || isSystemResource(resource)) {
			return false;
		}

		return withoutRetryAndDLQ(resource).startsWith(namespace + NAMESPACE_SEPARATOR);
	}

	public static String wrapNamespaceAndRetry(String namespace, String consumerGroup) {
		if (StringUtils.isEmpty(consumerGroup)) {
			return null;
		}

		return new StringBuilder()
				.append(MixAll.RETRY_GROUP_TOPIC_PREFIX)
				.append(wrapNamespace(namespace, consumerGroup))
				.toString();
	}

	public static String getNamespaceFromResource(String resource) {
		if (StringUtils.isEmpty(resource) || isSystemResource(resource)) {
			return STRING_BLANK;
		}

		String resourceWithoutRetryAndDLQ = withoutRetryAndDLQ(resource);
		int index = resourceWithoutRetryAndDLQ.indexOf(NAMESPACE_SEPARATOR);

		return index > 0 ? resourceWithoutRetryAndDLQ.substring(0, index) : STRING_BLANK;
	}

	public static String withoutRetryAndDLQ(String originalResource) {
		if (StringUtils.isEmpty(originalResource)) {
			return STRING_BLANK;
		}

		if (isRetryTopic(originalResource)) {
			return originalResource.substring(RETRY_PREFIX_LENGTH);
		}

		if (isDLQTopic(originalResource)) {
			return originalResource.substring(DLQ_PREFIX_LENGTH);
		}

		return originalResource;
	}

	private static boolean isSystemResource(String resource) {
		if (StringUtils.isEmpty(resource)) {
			return false;
		}

		if (TopicValidator.isSystemTopic(resource) || MixAll.isSysConsumerGroup(resource)) {
			return true;
		}
		return false;
	}

	public static boolean isRetryTopic(String resource) {
		return StringUtils.isNotBlank(resource) && resource.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
	}

	public static boolean isDLQTopic(String resource) {
		return StringUtils.isNotBlank(resource) && resource.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX); 
	}
}
