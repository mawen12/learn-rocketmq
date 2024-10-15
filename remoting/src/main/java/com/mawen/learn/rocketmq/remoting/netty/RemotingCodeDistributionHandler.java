package com.mawen.learn.rocketmq.remoting.netty;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.commons.collections.MapUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
@ChannelHandler.Sharable
public class RemotingCodeDistributionHandler extends ChannelDuplexHandler {

	private final ConcurrentMap<Integer, LongAdder> inboundDistribution;

	private final ConcurrentMap<Integer, LongAdder> outBoundDistribution;

	public RemotingCodeDistributionHandler() {
		this.inboundDistribution = new ConcurrentHashMap<>();
		this.outBoundDistribution = new ConcurrentHashMap<>();
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof RemotingCommand) {
			RemotingCommand cmd = (RemotingCommand) msg;
			countInbound(cmd.getCode());
		}
		ctx.fireChannelRead(msg);
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (msg instanceof RemotingCommand) {
			RemotingCommand cmd = (RemotingCommand) msg;
			countOutbound(cmd.getCode());
		}
		ctx.write(msg, promise);
	}

	public String getInboundSnapshotString() {
		return this.snapshotToString(this.getDistributionSnapshot(this.inboundDistribution));
	}

	public String getOutboundSnapshotString() {
		return this.snapshotToString(this.getDistributionSnapshot(this.outBoundDistribution));
	}

	private void countInbound(int requestCode) {
		LongAdder item = inboundDistribution.computeIfAbsent(requestCode, k -> new LongAdder());
		item.increment();
	}

	private void countOutbound(int requestCode) {
		LongAdder item = outBoundDistribution.computeIfAbsent(requestCode, k -> new LongAdder());
		item.increment();
	}

	private Map<Integer, Long> getDistributionSnapshot(Map<Integer, LongAdder> countMap) {
		return countMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().sumThenReset()));
	}

	private String snapshotToString(Map<Integer, Long> distribution) {
		if (MapUtils.isNotEmpty(distribution)) {
			StringBuilder sb = new StringBuilder("{");
			boolean first = true;

			for (Map.Entry<Integer, Long> entry : distribution.entrySet()) {
				if (0L == entry.getValue()) {
					continue;
				}

				sb.append(first ? "" : ", ").append(entry.getKey()).append(":").append(entry.getValue());
				first = false;
			}

			if (first) {
				return null;
			}
			sb.append("}");
			return sb.toString();
		}
		return null;
	}
}
