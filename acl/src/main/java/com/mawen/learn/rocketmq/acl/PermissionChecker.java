package com.mawen.learn.rocketmq.acl;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/2
 */
public interface PermissionChecker {

	void check(AccessResource checkedAccess, AccessResource ownedAccess);
}
