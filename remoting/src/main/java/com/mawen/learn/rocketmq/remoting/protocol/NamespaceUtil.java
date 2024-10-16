package com.mawen.learn.rocketmq.remoting.protocol;

import com.mawen.learn.rocketmq.common.MixAll;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class NamespaceUtil {
	public static final char NAMESPACE_SEPARATOR = '%';
	public static final String STRING_BLANK = "";
	public static final int RETRY_PREFIX_LENGTH = MixAll.RETRY_GROUP_TOPIC_PREFIX.length();
	public static final int DLQ_PREFIX_LENGTH = MixAll.DLQ_GROUP_TOPIC_PREFIX.length();

	TODO
}
