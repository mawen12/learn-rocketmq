package com.mawen.learn.rocketmq.acl.plain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/3
 */
public class PlainPermissionManager {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

	private String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

	private String defaultAclDir;
	private String defaultAclFile;
	private Map<String, Map<String, PlainAccessResource>> aclPlainAccessResourceMap = new HashMap<>();
	private Map<String, String> accessKeyTable = new HashMap<>();
	private List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();
	private RemoteAddressStrategyFactory remoteAddressStrategyFactory =
}
