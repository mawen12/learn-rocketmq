package com.mawen.learn.rocketmq.acl.common;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/2
 */
public class AclConstants {

	public static final String CONFIG_GLOBAL_WHITE_ADDRS = "globalWhiteRemoteAddresses";

	public static final String CONFIG_ACCOUNTS = "accounts";

	public static final String CONFIG_ACCESS_KEY = "accessKey";

	public static final String CONFIG_SECRET_KEY = "secretKey";

	public static final String CONFIG_WHITE_ADDR = "whiteRemoteAddress";

	public static final String CONFIG_ADMIN_ROLE = "admin";

	public static final String CONFIG_DEFAULT_TOPIC_PERM = "defaultTopicPerm";

	public static final String CONFIG_DEFAULT_GROUP_PERM = "defaultGroupPerm";

	public static final String CONFIG_TOPIC_PERMS = "topicPerms";

	public static final String CONFIG_GROUP_PERMS = "groupPerms";

	public static final String CONFIG_DATA_VERSION = "dataVersion";

	public static final String CONFIG_COUNTER = "counter";

	public static final String CONFIG_TIME_STAMP = "timestamp";

	public static final String PUB = "PUB";

	public static final String SUB = "SUB";

	public static final String DENY = "DENY";

	public static final String PUB_SUB = "PUB|SUB";

	public static final String SUB_PUB = "SUB|PUB";

	public static final int ACCESS_KEY_MIN_LENGTH = 6;

	public static final int SECRET_KEY_MIN_LENGTH = 6;
}
