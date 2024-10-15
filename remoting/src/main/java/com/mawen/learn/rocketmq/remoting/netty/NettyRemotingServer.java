package com.mawen.learn.rocketmq.remoting.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.constant.HAProxyConstants;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.BinaryUtil;
import com.mawen.learn.rocketmq.common.utils.NetworkUtil;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import com.mawen.learn.rocketmq.remoting.ChannelEventListener;
import com.mawen.learn.rocketmq.remoting.InvokeCallback;
import com.mawen.learn.rocketmq.remoting.RemotingServer;
import com.mawen.learn.rocketmq.remoting.common.RemotingHelper;
import com.mawen.learn.rocketmq.remoting.common.TlsMode;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.sctp.nio.NioSctpServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.ProtocolDetectionResult;
import io.netty.handler.codec.ProtocolDetectionState;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyTLV;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);
	private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_TRAFFIC_NAME);

	public static final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";
	public static final String HA_PROXY_DECODER = "HAProxyDecoder";
	public static final String HA_PROXY_HANDLER = "HAProxyHandler";
	public static final String TLS_MODE_HANDLER = "TlsModeHandler";
	public static final String TLS_HANDLER_NAME = "sslHandler";
	public static final String FILE_REGION_ENCODER_NAME = "fileRegionEncoder";

	private final ServerBootstrap serverBootstrap;

	private final EventLoopGroup eventLoopGroupSelector;

	private final EventLoopGroup eventLoopGroupBoss;

	private final NettyServerConfig nettyServerConfig;

	private final ExecutorService publicExecutor;

	private final ScheduledExecutorService scheduledExecutorService;

	private final ChannelEventListener channelEventListener;

	private final HashedWheelTimer timer = new HashedWheelTimer(r -> new Thread(r, "ServerHouseKeepingService"));

	private DefaultEventExecutorGroup defaultEventExecutorGroup;

	private final ConcurrentMap<Integer, NettyRemotingAbstract> remotingServerTable = new ConcurrentHashMap<>();

	private TlsModeHandler tlsModeHandler;
	private NettyEncoder encoder;
	private NettyConnectManageHandler connectionManageHandler;
	private NettyServerHandler serverHandler;
	private RemotingCodeDistributionHandler distributionHandler;

	public NettyRemotingServer(NettyServerConfig nettyServerConfig) {
		this(nettyServerConfig, null);
	}

	public NettyRemotingServer(NettyServerConfig nettyServerConfig, ChannelEventListener channelEventListener) {
		super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());

		this.serverBootstrap = new ServerBootstrap();
		this.nettyServerConfig = nettyServerConfig;
		this.channelEventListener = channelEventListener;
		this.publicExecutor = buildPublicExecutor(nettyServerConfig);
		this.scheduledExecutorService = buildScheduleExecutor();
		this.eventLoopGroupBoss = buildBossEventLoopGroup();
		this.eventLoopGroupSelector = buildEventLookGroupSelector();

		loadSslContext();
	}

	@Override
	public void start() {
		this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactoryImpl("NettyServerCodecThread_"));

		prepareSharableHandlers();

		serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
				.channel(useEpoll() ? EpollServerSocketChannel.class : NioSctpServerChannel.class)
				.option(ChannelOption.SO_BACKLOG, 1024)
				.option(ChannelOption.SO_REUSEADDR, true)
				.childOption(ChannelOption.SO_KEEPALIVE, false)
				.childOption(ChannelOption.TCP_NODELAY, true)
				.localAddress(new InetSocketAddress(this.nettyServerConfig.getBindAddress(), this.nettyServerConfig.getListenPort()))
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel socketChannel) throws Exception {
						configChanel(socketChannel);
					}
				});

		addCustomConfig(serverBootstrap);

		try {
			ChannelFuture sync = serverBootstrap.bind().sync();
			InetSocketAddress address = (InetSocketAddress) sync.channel().localAddress();
			if (0 == nettyServerConfig.getListenPort()) {
				this.nettyServerConfig.setListenPort(address.getPort());
			}
			log.info("RemotingServer started, listening {}:{}", nettyServerConfig.getBindAddress(), nettyServerConfig.getListenPort());
			this.remotingServerTable.put(this.nettyServerConfig.getListenPort(), this);
		}
		catch (Exception e) {
			throw new IllegalStateException(String.format("Failed to bind to %s:%d", nettyServerConfig.getBindAddress(), nettyServerConfig.getListenPort()), e);
		}

		if (this.channelEventListener != null) {
			this.nettyEventExecutor.start();
		}

		TimerTask timerScanResponseTable = new TimerTask() {
			@Override
			public void run(Timeout timeout) throws Exception {
				try {
					NettyRemotingServer.this.scanResponseTable();
				}
				catch (Throwable e) {
					log.error("scanResponseTable exception", e);
				}
				finally {
					timer.newTimeout(this, 1000, TimeUnit.MILLISECONDS);
				}
			}
		};
		this.timer.newTimeout(timerScanResponseTable, 3 * 1000, TimeUnit.MILLISECONDS);

		this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
			try {
				NettyRemotingServer.this.printRemotingCodeDistribution();
			}
			catch (Throwable e) {
				TRACE_LOGGER.error("NettyRemotingServer print remoting code distribution exception", e);
			}
		}, 1, 1, TimeUnit.SECONDS);
	}

	@Override
	public void shutdown() {
		try {
			if (nettyServerConfig.isEnableShutdownGracefully() && isShuttingDown.compareAndSet(false, true)) {
				Thread.sleep(Duration.ofSeconds(nettyServerConfig.getShutdownWaitTimeSeconds()).toMillis());
			}

			this.timer.stop();

			this.eventLoopGroupBoss.shutdownGracefully();

			this.eventLoopGroupSelector.shutdownGracefully();

			this.nettyEventExecutor.shutdown();

			if (this.defaultEventExecutorGroup != null) {
				this.defaultEventExecutorGroup.shutdownGracefully();
			}
		}
		catch (Exception e) {
			log.error("NettyRemotingServer shutdown exception", e);
		}

		if (this.publicExecutor != null) {
			try {
				this.publicExecutor.shutdown();
			}
			catch (Exception e) {
				log.error("NettyRemotingServer shutdown exception", e);
			}
		}
	}

	@Override
	public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
		ExecutorService executorThis = executor;
		if (executor == null) {
			executorThis = this.publicExecutor;
		}

		Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
		this.processorTable.put(requestCode, pair);
	}

	@Override
	public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
		this.defaultRequestProcessorPair = new Pair<>(processor, executor);
	}

	@Override
	public int localListenPort() {
		return this.nettyServerConfig.getListenPort();
	}

	@Override
	public Pair<NettyRequestProcessor, ExecutorService> getProcessor(int requestCode) {
		return processorTable.get(requestCode);
	}

	@Override
	public Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessor() {
		return defaultRequestProcessorPair;
	}

	@Override
	public RemotingServer newRemotingServer(int port) {
		SubRemotingServer remotingServer = new SubRemotingServer(port, this.nettyServerConfig.getServerOnewaySemaphoreValue(), this.nettyServerConfig.getServerAsyncSemaphoreValue());
		NettyRemotingAbstract existingServer = this.remotingServerTable.putIfAbsent(port, remotingServer);
		if (existingServer != null) {
			throw new RuntimeException("The port " + port + " already in use by another RemotingServer");
		}
		return remotingServer;
	}

	@Override
	public void removeRemotingServer(int port) {
		this.remotingServerTable.remove(port);
	}

	@Override
	public RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
		return this.invokeSyncImpl(channel, request, timeoutMillis);
	}

	@Override
	public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingSendRequestException, RemotingTimeoutException {
		this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
	}

	@Override
	public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
		this.invokeOnewayImpl(channel, request, timeoutMillis);
	}

	@Override
	public ChannelEventListener getChannelEventListener() {
		return channelEventListener;
	}

	@Override
	public ExecutorService getCallbackExecutor() {
		return publicExecutor;
	}

	protected ChannelPipeline configChanel(SocketChannel sc) {
		return sc.pipeline()
				.addLast(nettyServerConfig.isServerNettyWorkerGroupEnable() ? defaultEventExecutorGroup : null, HANDSHAKE_HANDLER_NAME, new HandshakeHandler())
				.addLast(nettyServerConfig.isServerNettyWorkerGroupEnable() ? defaultEventExecutorGroup : null, encoder, new NettyDecoder(), distributionHandler, new IdleStateHandler(0,0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()), connectionManageHandler, serverHandler);
	}

	private void addCustomConfig(ServerBootstrap childHandler) {
		if (nettyServerConfig.getServerSocketSndBufSize() > 0) {
			log.info("server set SO_SNDBUF to {}", nettyServerConfig.getServerSocketSndBufSize());
			childHandler.childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize());
		}

		if (nettyServerConfig.getServerSocketRcvBufSize() > 0) {
			log.info("server set SO_RCVBUF to {}", nettyServerConfig.getServerSocketRcvBufSize());
			childHandler.childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize());
		}

		if (nettyServerConfig.getWriteBufferLowWaterMark() > 0 && nettyServerConfig.getWriteBufferHighWaterMark() > 0) {
			log.info("server set netty WRITE_BUFFER_WATER_MARK to {},{}", nettyServerConfig.getWriteBufferLowWaterMark(), nettyServerConfig.getWriteBufferHighWaterMark());
			childHandler.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(nettyServerConfig.getWriteBufferLowWaterMark(), nettyServerConfig.getWriteBufferHighWaterMark()));
		}

		if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
			childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
		}
	}

	private EventLoopGroup buildBossEventLoopGroup() {
		if (useEpoll()) {
			return new EpollEventLoopGroup(1, new ThreadFactoryImpl("NettyEPOLLBoss_"));
		}
		else {
			return new NioEventLoopGroup(1, new ThreadFactoryImpl("NettyNIOBoss_"));
		}
	}

	private EventLoopGroup buildEventLookGroupSelector() {
		if (useEpoll()) {
			return new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactoryImpl("NettyServerEPOLLSelector_"));
		}
		else {
			return new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactoryImpl("NettyServerNIOSelector_"));
		}
	}

	private ExecutorService buildPublicExecutor(NettyServerConfig nettyServerConfig) {
		int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
		if (publicThreadNums <= 0) {
			publicThreadNums = 4;
		}

		return Executors.newFixedThreadPool(publicThreadNums, new ThreadFactoryImpl("NettyServerPublicExecutor_"));
	}

	private ScheduledExecutorService buildScheduleExecutor() {
		return ThreadUtils.newScheduledThreadPool(1, new ThreadFactoryImpl("NettyServerScheduler_", true), new ThreadPoolExecutor.DiscardOldestPolicy());
	}

	private void loadSslContext() {
		TlsMode tlsMode = TlsSystemConfig.tlsMode;
		log.info("Server is running is TLS {} mode", tlsMode.getName());

		if (tlsMode != TlsMode.DISABLED) {
			try {
				sslContext = TlsHelper.buildSslContext(false);
				log.info("SSLContext created for server");
			}
			catch (CertificateException | IOException e) {
				log.error("Failed to create SSLContext for server",e);
			}
		}
	}

	private boolean useEpoll() {
		return NetworkUtil.isLinuxPlatform()
				&& nettyServerConfig.isUseEpollNativeSelector()
				&& Epoll.isAvailable();
	}

	private void prepareSharableHandlers() {
		tlsModeHandler = new TlsModeHandler(TlsSystemConfig.tlsMode);
		encoder = new NettyEncoder();
		connectionManageHandler = new NettyConnectManageHandler();
		serverHandler = new NettyServerHandler();
		distributionHandler = new RemotingCodeDistributionHandler();
	}

	private void printRemotingCodeDistribution() {
		if (distributionHandler != null) {
			String inboundSnapshotString = distributionHandler.getInboundSnapshotString();
			if (inboundSnapshotString != null) {
				TRACE_LOGGER.info("Port: {}, RequestCode Distribution: {}", nettyServerConfig.getListenPort(), inboundSnapshotString);
			}

			String outboundSnapshotString = distributionHandler.getOutboundSnapshotString();
			if (outboundSnapshotString != null) {
				TRACE_LOGGER.info("Port: {}, ResponseCode Distribution: {}", nettyServerConfig.getListenPort(), outboundSnapshotString);
			}
		}
	}

	public DefaultEventExecutorGroup getDefaultEventExecutorGroup() {
		return defaultEventExecutorGroup;
	}

	public NettyEncoder getEncoder() {
		return encoder;
	}

	public NettyConnectManageHandler getConnectionManageHandler() {
		return connectionManageHandler;
	}

	public NettyServerHandler getServerHandler() {
		return serverHandler;
	}

	public RemotingCodeDistributionHandler getDistributionHandler() {
		return distributionHandler;
	}

	public class HandshakeHandler extends ByteToMessageDecoder {

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> list) throws Exception {
			try {
				ProtocolDetectionResult<HAProxyProtocolVersion> detectionResult = HAProxyMessageDecoder.detectProtocol(byteBuf);
				if (detectionResult.state() == ProtocolDetectionState.NEEDS_MORE_DATA) {
					return;
				}

				if (detectionResult.state() == ProtocolDetectionState.DETECTED) {
					ctx.pipeline()
							.addAfter(defaultEventExecutorGroup, ctx.name(), HA_PROXY_DECODER, new HAProxyMessageDecoder())
							.addAfter(defaultEventExecutorGroup, HA_PROXY_DECODER, HA_PROXY_HANDLER, new HAProxyMessageHandler())
							.addAfter(defaultEventExecutorGroup, HA_PROXY_HANDLER, TLS_MODE_HANDLER, tlsModeHandler);
				}
				else {
					ctx.pipeline()
							.addAfter(defaultEventExecutorGroup, ctx.name(), TLS_MODE_HANDLER, tlsModeHandler);
				}

				try {
					ctx.pipeline().remove(this);
				}
				catch (NoSuchElementException e) {
					log.error("Error while removing HandshakeHandler", e);
				}
			}
			catch (Exception e) {
				log.error("process proxy protocol negotiator failed.", e);
				throw e;
			}
		}
	}

	@ChannelHandler.Sharable
	public class TlsModeHandler extends SimpleChannelInboundHandler<ByteBuf> {

		private static final byte HANDSHAKE_MAGIC_CODE = 0x16;

		private final TlsMode tlsMode;

		public TlsModeHandler(TlsMode tlsMode) {
			this.tlsMode = tlsMode;
		}

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
			byte b = msg.getByte(msg.readerIndex());

			if (b == HANDSHAKE_MAGIC_CODE) {
				switch (tlsMode) {
					case DISABLED:
						ctx.close();
						log.warn("Clients intend to establish an SSL connection while this server is running in SSL disabled mode");
						throw new UnsupportedOperationException("The NettyRemotingServer in SSL disabled mode doesn't support ssl client");
					case PERMISSIVE:
					case ENFORCING:
						if (sslContext != null) {
							ctx.pipeline()
									.addAfter(defaultEventExecutorGroup, TLS_MODE_HANDLER, TLS_HANDLER_NAME, sslContext.newHandler(ctx.channel().alloc()))
									.addAfter(defaultEventExecutorGroup, TLS_HANDLER_NAME, FILE_REGION_ENCODER_NAME, new FileRegionEncoder());
							log.info("Handlers prepended to channel pipeline to establish SSL connection");
						}
						else {
							ctx.close();
							log.error("Trying to establish an SSL connection but sslContext is null");
						}
						break;
					default:
						log.warn("Unknown TLS mode");
						break;
				}
			}
			else if (tlsMode == TlsMode.ENFORCING) {
				ctx.close();
				log.warn("Clients intend to establish an SSL connection while this server is running in SSL disabled mode");
			}

			try {
				ctx.pipeline().remove(this);
			}
			catch (NoSuchElementException e) {
				log.error("Error while removing TlsModeHandler", e);
			}

			ctx.fireChannelRead(msg.retain());
		}
	}

	@ChannelHandler.Sharable
	public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
			Integer localPort = RemotingHelper.parseSocketAddressPort(ctx.channel().localAddress());
			NettyRemotingAbstract remotingAbstract = NettyRemotingServer.this.remotingServerTable.get(localPort);

			if (localPort != -1 && remotingAbstract != null) {
				remotingAbstract.processMessageReceived(ctx, msg);
				return;
			}
			RemotingHelper.closeChannel(ctx.channel());
		}

		@Override
		public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
			Channel channel = ctx.channel();
			if (channel.isWritable()) {
				if (!channel.config().isAutoRead()) {
					channel.config().setAutoRead(true);
					log.info("Channel[{}] turns writable, bytes to buffer before changing channel to un-writable: {}",
							RemotingHelper.parseChannelRemoteAddr(channel), channel.bytesBeforeUnwritable());
				}
			}
			else {
				channel.config().setAutoRead(false);
				log.warn("Channel[{}] auto-read is disabled, bytes to drain before it turns writable: {}",
						RemotingHelper.parseChannelRemoteAddr(channel), channel.bytesBeforeWritable());
			}
			super.channelWritabilityChanged(ctx);
		}
	}

	@ChannelHandler.Sharable
	public class NettyConnectManageHandler extends ChannelDuplexHandler {

		@Override
		public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
			String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
			log.info("NETTY SERVER PIPELINE: channelRegistered {}", remoteAddr);
			super.channelRegistered(ctx);
		}

		@Override
		public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
			String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
			log.info("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddr);
			super.channelUnregistered(ctx);
		}

		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
			log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]", remoteAddr);
			super.channelActive(ctx);

			if (NettyRemotingServer.this.channelEventListener != null) {
				NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddr, ctx.channel()));
			}
		}

		@Override
		public void channelInactive(ChannelHandlerContext ctx) throws Exception {
			String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
			log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]", remoteAddr);
			super.channelInactive(ctx);

			if (NettyRemotingServer.this.channelEventListener != null) {
				NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddr, ctx.channel()));
			}
		}

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
			if (evt instanceof IdleStateEvent) {
				IdleStateEvent event = (IdleStateEvent) evt;
				if (event.state().equals(IdleState.ALL_IDLE)) {
					String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
					log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]", remoteAddr);
					RemotingHelper.closeChannel(ctx.channel());
					if (NettyRemotingServer.this.channelEventListener != null) {
						NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddr, ctx.channel()));
					}
				}
			}
			super.userEventTriggered(ctx, evt);
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
			String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
			log.warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddr);
			log.warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);

			if (NettyRemotingServer.this.channelEventListener != null) {
				NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddr, ctx.channel()));
			}

			RemotingHelper.closeChannel(ctx.channel());
		}
	}

	public class HAProxyMessageHandler extends ChannelInboundHandlerAdapter {
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
			if (msg instanceof HAProxyMessage) {
				handleWithMessage((HAProxyMessage) msg, ctx.channel());
			}
			else {
				super.channelRead(ctx, msg);
			}

			ctx.pipeline().remove(this);
		}

		private void handleWithMessage(HAProxyMessage msg, Channel channel) {
			try {
				if (StringUtils.isNotBlank(msg.sourceAddress())) {
					RemotingHelper.setPropertyToAttr(channel, AttributeKeys.PROXY_PROTOCOL_ADDR, msg.sourceAddress());
				}
				if (msg.sourcePort() > 0) {
					RemotingHelper.setPropertyToAttr(channel, AttributeKeys.PROXY_PROTOCOL_PORT, String.valueOf(msg.sourcePort()));
				}
				if (StringUtils.isNotBlank(msg.destinationAddress())) {
					RemotingHelper.setPropertyToAttr(channel, AttributeKeys.PROXY_PROTOCOL_SERVER_ADDR, msg.destinationAddress());
				}
				if (msg.destinationPort() > 0) {
					RemotingHelper.setPropertyToAttr(channel, AttributeKeys.PROXY_PROTOCOL_SERVER_PORT, String.valueOf(msg.destinationPort()));
				}
				if (CollectionUtils.isNotEmpty(msg.tlvs())) {
					msg.tlvs().forEach(tlv -> handleHAProxyTLV(tlv, channel));
				}
			}
			finally {
				msg.release();
			}
		}
	}

	class SubRemotingServer extends NettyRemotingAbstract implements RemotingServer {

		private volatile int listenPort;

		private volatile Channel serverChannel;

		public SubRemotingServer(final int port, final int permitsOneway, final int permitsAsync) {
			super(permitsOneway, permitsAsync);
			this.listenPort = port;
		}

		@Override
		public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
			ExecutorService executorThis = executor;
			if (executor == null) {
				executorThis = NettyRemotingServer.this.publicExecutor;
			}

			Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
			this.processorTable.put(requestCode, pair);
		}

		@Override
		public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
			this.defaultRequestProcessorPair = new Pair<>(processor, executor);
		}

		@Override
		public int localListenPort() {
			return listenPort;
		}

		@Override
		public Pair<NettyRequestProcessor, ExecutorService> getProcessor(int requestCode) {
			return this.processorTable.get(requestCode);
		}

		@Override
		public Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessor() {
			return this.defaultRequestProcessorPair;
		}

		@Override
		public RemotingServer newRemotingServer(int port) {
			throw new UnsupportedOperationException("The SubRemotingServer of NettyRemotingServer doesn't support new nested RemotingServer");
		}

		@Override
		public void removeRemotingServer(int port) {
			throw new UnsupportedOperationException("The SubRemotingServer of NettyRemotingServer doesn't support remove nested RemotingServer");
		}

		@Override
		public RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
			return this.invokeSyncImpl(channel, request, timeoutMillis);
		}

		@Override
		public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingSendRequestException, RemotingTimeoutException {
			this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
		}

		@Override
		public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
			this.invokeOnewayImpl(channel, request, timeoutMillis);
		}

		@Override
		public void start() {
			try {
				if (listenPort < 0) {
					listenPort = 0;
				}

				this.serverChannel = NettyRemotingServer.this.serverBootstrap.bind(listenPort).sync().channel();
				if (listenPort == 0) {
					InetSocketAddress address = (InetSocketAddress) this.serverChannel.localAddress();
					this.listenPort = address.getPort();
				}
			}
			catch (InterruptedException e) {
				throw new RuntimeException("this.subRemotingServer.serverBootstrap.bind().sync() InterruptedException", e);
			}
		}

		@Override
		public void shutdown() {
			isShuttingDown.set(true);
			if (this.serverChannel != null) {
				try {
					this.serverChannel.close().await(5, TimeUnit.SECONDS);
				}
				catch (InterruptedException ignored) {

				}
			}
		}

		@Override
		public ChannelEventListener getChannelEventListener() {
			return NettyRemotingServer.this.getChannelEventListener();
		}

		@Override
		public ExecutorService getCallbackExecutor() {
			return NettyRemotingServer.this.getCallbackExecutor();
		}
	}

	protected void handleHAProxyTLV(HAProxyTLV tlv, Channel channel) {
		byte[] bytes = ByteBufUtil.getBytes(tlv.content());
		if (!BinaryUtil.isAscii(bytes)) {
			return;
		}

		AttributeKey<String> key = AttributeKeys.valueOf(HAProxyConstants.PROXY_PROTOCOL_TLV_PREFIX + String.format("%02x", tlv.typeByteValue()));
		RemotingHelper.setPropertyToAttr(channel, key, new String(bytes, CharsetUtil.UTF_8));
	}
}
