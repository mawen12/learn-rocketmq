package com.mawen.learn.rocketmq.remoting.protocol;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class MQProtocolHelper {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

	public static boolean registerBrokerToNameServer(final String nsAddr, final String brokerAddr, final long timeoutMillis) {
		RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
		requestHeader.setBrokerAddr(brokerAddr);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, requestHeader);

		try {
			RemotingCommand response = RemotingHelper.invokeSync(nsAddr, request, timeoutMillis);
			if (response != null) {
				return ResponseCode.SUCCESS == response.getCode();
			}
		}
		catch (Exception e) {
			log.error("Failed to register broker", e);
		}
		return false;
	}
}
