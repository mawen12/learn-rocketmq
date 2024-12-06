package com.mawen.learn.rocketmq.acl.plain;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/2
 */
public interface RemoteAddressStrategy {

	boolean match(PlainAccessResource plainAccessResource);

}
