package com.mawen.learn.rocketmq.acl.plain;

import java.util.HashSet;
import java.util.Set;

import com.mawen.learn.rocketmq.acl.common.AclException;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/3
 */
public class RemoteAddressStrategyFactory {

	private static final Logger log = LoggerFactory.getLogger(RemoteAddressStrategyFactory.class);

	public static final NullRemoteAddressStrategy

	public static class NullRemoteAddressStrategy implements RemoteAddressStrategy {

		@Override
		public boolean match(PlainAccessResource plainAccessResource) {
			return true;
		}
	}

	public static class BlankRemoteAddressStrategy implements RemoteAddressStrategy {
		@Override
		public boolean match(PlainAccessResource plainAccessResource) {
			return false;
		}
	}

	public static class MultipleRemoteAddressStrategy implements RemoteAddressStrategy {

		private final Set<String> multipleSet = new HashSet<>();

		public MultipleRemoteAddressStrategy(String[] strArray) {
			InetAddressValidator validator = InetAddressValidator.getInstance();
			for (String netAddress : strArray) {
				if (validator.isValidInet4Address(netAddress)) {
					multipleSet.add(netAddress);
				}
				else if (validator.isValidInet6Address(netAddress)) {
					multipleSet.add(AclUtils.expandIP(netAddress, 8));
				}
				else {
					throw new AclException(String.format("NetAddress examine Exception netAddress is %s",netAddress));
				}
			}
		}

		@Override
		public boolean match(PlainAccessResource plainAccessResource) {
			InetAddressValidator validator = InetAddressValidator.getInstance();
			plainAccessResource.getW
			return false;
		}
	}
}



