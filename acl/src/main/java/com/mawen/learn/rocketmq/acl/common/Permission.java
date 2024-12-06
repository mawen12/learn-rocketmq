package com.mawen.learn.rocketmq.acl.common;

import java.util.HashSet;
import java.util.Set;

import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/2
 */
public class Permission {

	public static final byte DENY = 1;
	public static final byte ANY = 1 << 1;
	public static final byte PUB = 1 << 2;
	public static final byte SUB = 1 << 3;

	public static final Set<Integer> ADMIN_CODE = new HashSet<>();

	static {
		ADMIN_CODE.add(RequestCode.UPDATE_AND_CREATE_TOPIC);
		ADMIN_CODE.add(RequestCode.UPDATE_BROKER_CONFIG);
		ADMIN_CODE.add(RequestCode.DELETE_TOPIC_IN_BROKER);
		ADMIN_CODE.add(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP);
		ADMIN_CODE.add(RequestCode.DELETE_SUBSCRIPTIONGROUP);
		ADMIN_CODE.add(RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC);
		ADMIN_CODE.add(RequestCode.UPDATE_AND_CREATE_ACL_CONFIG);
		ADMIN_CODE.add(RequestCode.DELETE_ACL_CONFIG);
		ADMIN_CODE.add(RequestCode.GET_BROKER_CLUSTER_ACL_INFO);
	}

	public static boolean checkPermission(byte neededPerm, byte ownedPerm) {
		if ((ownedPerm & DENY) > 0) {
			return false;
		}
		if ((neededPerm & ANY) > 0) {
			return (ownedPerm & PUB) > 0 || (ownedPerm & SUB) > 0;
		}
		return (neededPerm & ownedPerm) > 0;
	}

	public static byte parsePermFromString(String permString) {
		if (permString == null) {
			return Permission.ANY;
		}
		switch (permString.trim()) {
			case AclConstants.PUB:
				return Permission.PUB;
			case AclConstants.SUB:
				return Permission.SUB;
			case AclConstants.PUB_SUB:
			case AclConstants.SUB_PUB:
				return Permission.PUB | Permission.SUB;
			case AclConstants.DENY:
				return Permission.DENY;
			default:
				return Permission.DENY;
		}
	}

	public static void parseResourcePerms(PlainAccessResource ) {

	}
}
