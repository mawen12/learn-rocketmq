package com.mawen.learn.rocketmq.common.utils;

import java.util.regex.Pattern;

import com.mawen.learn.rocketmq.common.MixAll;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class NameServerAddressUtils {

	public static final String INSTANCE_PREFIX = "MQ_INST_";
	public static final String INSTANCE_REGEX = INSTANCE_PREFIX + "\\w+_\\w+";
	public static final String ENDPOINT_PREFIX = "(\\w+:\\|)";
	public static final Pattern NAMESRV_ENDPOINT_PATTERN = Pattern.compile("^http://.*");
	public static final Pattern INST_ENDPOINT_PATTERN = Pattern.compile("^" + ENDPOINT_PREFIX + INSTANCE_REGEX + "\\..*");

	public static String getNameServerAddresses() {
		return System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
	}

	public static boolean validateInstanceEndpoint(String endpoint) {
		return INST_ENDPOINT_PATTERN.matcher(endpoint).matches();
	}

	public static String parseInstanceFromEndpoint(String endpoint) {
		if (StringUtils.isEmpty(endpoint)) {
			return null;
		}

		return endpoint.substring(endpoint.lastIndexOf("/") + 1, endpoint.indexOf('.'));
	}

	public static String getNameSrvAddrFromNamesrvEndpoint(String nameSrvEndpoint) {
		if (StringUtils.isEmpty(nameSrvEndpoint)) {
			return null;
		}

		return nameSrvEndpoint.substring(nameSrvEndpoint.lastIndexOf('/') + 1);
	}
}
