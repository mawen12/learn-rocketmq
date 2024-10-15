package com.mawen.learn.rocketmq.remoting.netty;

import com.google.common.base.Stopwatch;
import com.mawen.learn.rocketmq.remoting.common.RemotingHelper;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {

	private static final Logger log = LoggerFactory.getLogger(NettyDecoder.class);

	private static final int FRAME_MAX_LENGTH = Integer.parseInt(System.getProperty("com.mawen.learn.rocketmq.remoting.frameMaxLength", "16777216"));

	public NettyDecoder() {
		super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
	}

	@Override
	protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
		ByteBuf frame = null;
		Stopwatch timer = Stopwatch.createStarted();

		try {
			frame = (ByteBuf) super.decode(ctx, in);
			if (frame == null) {
				return null;
			}

			RemotingCommand cmd = RemotingCommand.decode(frame);
			cmd.setProcessTimer(timer);
			return cmd;
		}
		catch (Exception e) {
			log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
			RemotingHelper.closeChannel(ctx.channel());
		}
		finally {
			if (frame != null) {
				frame.release();
			}
		}
		return null;
	}
}
