package com.mawen.learn.rocketmq.remoting.netty;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class FileRegionEncoder extends MessageToByteEncoder<FileRegion> {

	@Override
	protected void encode(ChannelHandlerContext channelHandlerContext, FileRegion msg, ByteBuf out) throws Exception {
		WritableByteChannel writableByteChannel = new WritableByteChannel() {
			@Override
			public int write(ByteBuffer src) throws IOException {
				CompositeByteBuf buf = (CompositeByteBuf) out;
				ByteBuf unpooled = Unpooled.wrappedBuffer(src);
				buf.addComponent(true, unpooled);
				return unpooled.readableBytes();
			}

			@Override
			public boolean isOpen() {
				return true;
			}

			@Override
			public void close() throws IOException {
			}
		};

		long toTransfer = msg.count();

		while (true) {
			long transferred = msg.transferred();
			if (toTransfer - transferred <= 0) {
				break;
			}
			msg.transferTo(writableByteChannel, transferred);
		}
	}

	@Override
	protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, FileRegion msg, boolean preferDirect) throws Exception {
		ByteBufAllocator allocator = ctx.alloc();
		return preferDirect ? allocator.compositeDirectBuffer() : allocator.compositeHeapBuffer();
	}
}
