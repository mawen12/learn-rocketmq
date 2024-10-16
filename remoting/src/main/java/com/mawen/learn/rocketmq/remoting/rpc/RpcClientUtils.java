package com.mawen.learn.rocketmq.remoting.rpc;

import java.nio.ByteBuffer;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import io.netty.buffer.ByteBuf;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class RpcClientUtils {

	public static RemotingCommand createCommandForRpcRequest(RpcRequest request) {
		RemotingCommand cmd = RemotingCommand.createRequestCommand(request.getCode(), request.getHeader());
		cmd.setBody(encodeBody(request.getBody()));
		return cmd;
	}

	public static RemotingCommand createCommandForRpcResponse(RpcResponse response) {
		RemotingCommand cmd = RemotingCommand.createResponseCommandWithHeader(response.getCode(), response.getHeader());
		cmd.setRemark(response.getException() == null ? "" : response.getException().getMessage());
		cmd.setBody(encodeBody(response.getBody()));
		return cmd;
	}

	public static byte[] encodeBody(Object body) {
		if (body == null) {
			return null;
		}

		if (body instanceof byte[]) {
			return (byte[]) body;
		}
		else if (body instanceof RemotingSerializable) {
			return ((RemotingSerializable) body).encode();
		}
		else if (body instanceof ByteBuffer) {
			ByteBuffer buffer = (ByteBuffer) body;
			buffer.mark();
			byte[] data = new byte[buffer.remaining()];
			buffer.get(data);
			buffer.reset();
			return data;
		}
		else {
			throw new RuntimeException("Unsupported body type " + body.getClass());
		}
	}
}
