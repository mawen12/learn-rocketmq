package com.mawen.learn.rocketmq.acl.plain;

import java.util.Map;

import com.mawen.learn.rocketmq.acl.AccessResource;
import com.mawen.learn.rocketmq.acl.common.AclException;
import com.mawen.learn.rocketmq.acl.common.Permission;
import com.mawen.learn.rocketmq.acl.common.SessionCredentials;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/2
 */
@Getter
@Setter
@NoArgsConstructor
public class PlainAccessResource implements AccessResource {

	private String accessKey;
	private String secretKey;
	private String whiteRemoteAddress;
	private boolean admin;
	private byte defaultTopicPerm = 1;
	private byte defaultGroupPerm = 1;
	private Map<String, Byte> resourcePermMap;
	private RemoteAddressStrategy remoteAddressStrategy;
	private int requestCode;
	private byte[] content;
	private String signature;
	private String secretToken;
	private String recognition;

	public static PlainAccessResource parse(RemotingCommand request, String remoteAddr) {
		PlainAccessResource accessResource = new PlainAccessResource();
		if (remoteAddr != null && remoteAddr.contains(":")) {
			accessResource.setWhiteRemoteAddress(remoteAddr.substring(0, remoteAddr.lastIndexOf(':')));
		}
		else {
			accessResource.setWhiteRemoteAddress(remoteAddr);
		}

		accessResource.setRequestCode(request.getCode());

		if (request.getExtFields() == null) {
			return accessResource;
		}
		accessResource.setAccessKey(request.getExtFields().get(SessionCredentials.ACCESS_KEY));
		accessResource.setSignature(request.getExtFields().get(SessionCredentials.SIGNATURE));
		accessResource.setSecretToken(request.getExtFields().get(SessionCredentials.SECURITY_TOKEN));

		try {
			switch (request.getCode()) {
				case RequestCode.SEND_MESSAGE:
					String topic = request.getExtFields().get("topic");
					accessResource.addResourceAndPerm(topic, PlainAccessResource.isRetryTopic(topic) ? Permission.SUB : Permission.PUB);
					break;
			}
		}
		catch (Throwable t) {
			throw new AclException(t.getMessage(), t);
		}
	}
}
