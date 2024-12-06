package com.mawen.learn.rocketmq.acl;

import java.util.List;
import java.util.Map;

import com.google.protobuf.GeneratedMessageV3;
import com.mawen.learn.rocketmq.common.AclConfig;
import com.mawen.learn.rocketmq.common.PlainAccessConfig;
import com.mawen.learn.rocketmq.remoting.protocol.DataVersion;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import sun.net.www.protocol.http.AuthenticationHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/2
 */
public interface AccessValidator {

	AccessResource parse(RemotingCommand request, String remoteAddr);

	AccessResource parse(GeneratedMessageV3 messageV3, AuthenticationHeader header);

	void valid(AccessResource accessResource);

	boolean updateAccessConfig(PlainAccessConfig plainAccessConfig);

	boolean deleteAccessConfig(String accessKey);

	String getAclConfigVersion();

	boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList);

	boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList, String aclFileFullPath);

	AclConfig getAllAclConfig();

	Map<String, DataVersion> getAllAclConfigVersion();
}
