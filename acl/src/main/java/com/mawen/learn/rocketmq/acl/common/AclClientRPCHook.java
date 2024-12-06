package com.mawen.learn.rocketmq.acl.common;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/2
 */
@Getter
@AllArgsConstructor
public class AclClientRPCHook implements RPCHook {

	private final SessionCredentials sessionCredentials;

	@Override
	public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
		request.addExtField(SessionCredentials.ACCESS_KEY, sessionCredentials.getAccessKey());

		if (sessionCredentials.getSecurityToken() != null) {
			request.addExtField(SessionCredentials.SECURITY_TOKEN,sessionCredentials.getSecurityToken());
		}

		byte[] total = AclUtils.combineRequestContent(request, parseRequestContent(request));
		String signature = AclUtils.calSignature(total, sessionCredentials.getSecretKey());
		request.addExtField(SessionCredentials.SIGNATURE, signature);
	}

	@Override
	public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
		// NOP
	}

	protected SortedMap<String, String> parseRequestContent(RemotingCommand request) {
		request.makeCustomHeaderToNet();
		Map<String, String> extFields = request.getExtFields();
		return new TreeMap<>(extFields);
	}
}
