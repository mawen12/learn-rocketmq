package com.mawen.learn.rocketmq.remoting.netty;

import com.mawen.learn.rocketmq.remoting.common.RemotingHelper;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandler;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
@ChannelHandler.Sharable
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {

	private static final Logger log = LoggerFactory.getLogger(NettyEncoder.class);

	@Override
	protected void encode(ChannelHandlerContext ctx, RemotingCommand command, ByteBuf byteBuf) throws Exception {
		try {
			command.fastEncodeHeader(byteBuf);
			byte[] body = command.getBody();
			if (body != null) {
				byteBuf.writeBytes(byteBuf);
			}
		}
		catch (Exception e) {
			log.error("encode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
			if (command != null) {
				log.error(command.toString());
			}
			RemotingHelper.closeChannel(ctx.channel());
		}
	}
}
