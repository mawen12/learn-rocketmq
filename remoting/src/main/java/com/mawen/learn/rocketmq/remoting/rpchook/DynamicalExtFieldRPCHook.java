package com.mawen.learn.rocketmq.remoting.rpchook;

import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class DynamicalExtFieldRPCHook implements RPCHook {

	@Override
	public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
		String zoneName = System.getProperty(MixAll.ROCKETMQ_ZONE_PROPERTY, System.getenv(MixAll.ROCKETMQ_ZONE_ENV));
		if (StringUtils.isNotBlank(zoneName)) {
			request.addExtField(MixAll.ZONE_NAME, zoneName);
		}

		String zoneMode = System.getProperty(MixAll.ROCKETMQ_ZONE_MODE_PROPERTY, System.getenv(MixAll.ROCKETMQ_ZONE_MODE_ENV));
		if (StringUtils.isNotBlank(zoneMode)) {
			request.addExtField(MixAll.ZONE_MODE, zoneMode);
		}
	}

	@Override
	public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {

	}
}
