package com.mawen.learn.rocketmq.remoting.netty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.mawen.learn.rocketmq.common.AbortProcessException;
import com.mawen.learn.rocketmq.common.MQVersion;
import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.remoting.ChannelEventListener;
import com.mawen.learn.rocketmq.remoting.InvokeCallback;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.common.RemotingHelper;
import com.mawen.learn.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.mawen.learn.rocketmq.remoting.metrics.RemotingMetricsManager;
import com.mawen.learn.rocketmq.remoting.pipeline.RequestPipeline;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSysResponseCode;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import static com.mawen.learn.rocketmq.remoting.metrics.RemotingMetricsConstant.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
public abstract class NettyRemotingAbstract {

	private static final Logger log = LoggerFactory.getLogger(NettyRemotingAbstract.class);

	protected final Semaphore semaphoreOneway;

	protected final Semaphore semaphoreAsync;

	protected final ConcurrentMap<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<>(256);

	protected final Map<Integer, Pair<NettyRequestProcessor, ExecutorService>> processorTable = new HashMap<>(64);

	protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

	protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessorPair;

	protected volatile SslContext sslContext;

	protected List<RPCHook> rpcHooks = new ArrayList<>();

	protected RequestPipeline requestPipeline;

	protected AtomicBoolean isShuttingDown = new AtomicBoolean(false);

	static {
		NettyLogger.initNettyLogger();
	}

	public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
		this.semaphoreOneway = new Semaphore(permitsOneway, true);
		this.semaphoreAsync = new Semaphore(permitsAsync, true);
	}

	public abstract ChannelEventListener getChannelEventListener();

	public abstract ExecutorService getCallbackExecutor();

	public void putNettyEvent(final NettyEvent event) {
		this.nettyEventExecutor.putNettyEvent(event);
	}

	public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
		if (msg != null) {
			switch (msg.getType()) {
				case REQUEST_COMMAND:
					processRequestCommand(ctx, msg);
					break;
				case RESPONSE_COMMAND:
					processResponseCommand(ctx, msg);
					break;
				default:
					break;
			}
		}
	}

	protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
		if (this.rpcHooks.size() > 0) {
			for (RPCHook rpcHook : rpcHooks) {
				rpcHook.doBeforeRequest(addr, request);
			}
		}
	}

	public void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
		if (this.rpcHooks.size() > 0) {
			for (RPCHook rpcHook : rpcHooks) {
				rpcHook.doAfterResponse(addr, request, response);
			}
		}
	}

	public static void writeResponse(Channel channel, RemotingCommand request, RemotingCommand response) {
		writeResponse(channel, request, response, null);
	}

	public static void writeResponse(Channel channel, RemotingCommand request, RemotingCommand response, Consumer<Future<?>> callback) {
		if (response == null) {
			return;
		}

		AttributesBuilder attributesBuilder = RemotingMetricsManager.newAttributesBuilder()
				.put(LABEL_IS_LONG_POLLING, request.isSuspended())
				.put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
				.put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(request.getCode()));
		if (request.isOnewayRPC()) {
			attributesBuilder.put(LABEL_RESULT, RESULT_ONEWAY);
			RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MICROSECONDS), attributesBuilder.build());
			return;
		}

		response.setOpaque(request.getOpaque());
		response.markResponseType();

		try {
			channel.writeAndFlush(response).addListener(future -> {
				if (future.isSuccess()) {
					log.debug("Response[request code: {}, response code: {}, opaque: {}] is written to channel {}",
							request.getCode(), response.getCode(), response.getOpaque(), channel);
				}
				else {
					log.error("Failed to write response[request code: {},  response code: {}, opaque: {}] to channel{},",
							request.getCode(), response.getCode(), response.getOpaque(), channel, future.cause());
				}

				attributesBuilder.put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future));
				RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
				if (callback != null) {
					callback.accept(future);
				}
			});
		}
		catch (Throwable e) {
			log.error("process request over, but response failed", e);
			log.error(request.toString());
			log.error(response.toString());
			attributesBuilder.put(LABEL_RESULT, RESULT_WRITE_CHANNEL_FAILED);
			RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
		}
	}

	public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
		Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
		Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessorPair : matched;
		int opaque = cmd.getOpaque();

		if (pair == null) {
			String error = "request type " + cmd.getCode() + " not supported";
			RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
			response.setOpaque(opaque);
			writeResponse(ctx.channel(), cmd, response);
			log.info("proxy is shutting down, write response GO_AWAY, channel={}, requestCode={}, opaque={}", ctx.channel(), cmd.getCode(), opaque);
			return;
		}

		Runnable run = buildProcessRequestHandler(ctx, cmd, pair, opaque);
		if (isShuttingDown.get()) {
			if (cmd.getVersion() > MQVersion.Version.V5_1_4.ordinal()) {
				RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.GO_AWAY, "please go away");
				response.setOpaque(opaque);
				writeResponse(ctx.channel(), cmd, response);
				log.info("proxy is shutting down, write response GO_AWAY, channel={}, requestCode={}, opaque={}", ctx.channel(), cmd.getCode(), opaque);
				return;
			}
		}

		if (pair.getObject1().rejectRequest()) {
			RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY, "[REJECTREQUEST]system busy, start flow control for a while");
			response.setOpaque(opaque);
			writeResponse(ctx.channel(), cmd, response);
			return;
		}

		try {
			RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
			pair.getObject2().submit(requestTask);
		}
		catch (RejectedExecutionException e) {
			if (System.currentTimeMillis() % 10000 == 0) {
				log.warn("{}, too many requests and system thread pool busy, RejectedExecutionException {} request code {}",
						RemotingHelper.parseChannelRemoteAddr(ctx.channel()), pair.getObject2().toString(), cmd.getCode());
			}

			RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY, "[OVERLOAD]system busy, start flow control for a while");
			response.setOpaque(opaque);
			writeResponse(ctx.channel(), cmd, response);
		}
		catch (Throwable e) {
			Attributes attributes = RemotingMetricsManager.newAttributesBuilder()
					.put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(cmd.getCode()))
					.put(LABEL_RESULT, RESULT_PROCESS_REQUEST_FAILED)
					.build();
			RemotingMetricsManager.rpcLatency.record(cmd.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);
		}
	}

	public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
		int opaque = cmd.getOpaque();
		ResponseFuture future = responseTable.get(opaque);

		if (future != null) {
			future.setResponseCommand(cmd);
			responseTable.remove(future);

			if (future.getInvokeCallback() != null) {
				executeInvokeCallback(future);
			}
			else {
				future.putResponse(cmd);
				future.release();
			}
		}
		else {
			log.warn("receive response, cmd={}, but not matched any request, address={}, channelId={}",
					cmd, RemotingHelper.parseChannelRemoteAddr(ctx.channel()), ctx.channel().id());
		}
	}

	public void scanResponseTable() {
		List<ResponseFuture> rfList = new ArrayList<>();
		Iterator<Map.Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Integer, ResponseFuture> next = it.next();
			ResponseFuture rep = next.getValue();

			if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
				rep.release();
				it.remove();
				rfList.add(rep);
				log.warn("remove timeout request, " + rep);
			}
		}

		for (ResponseFuture future : rfList) {
			try {
				executeInvokeCallback(future);
			}
			catch (Throwable e) {
				log.warn("scanResponseTable, operationComplete Exception", e);
			}
		}
	}

	public RemotingCommand invokeSyncImpl(Channel channel, RemotingCommand request, long timeoutMillis) throws RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		try {
			return invokeImpl(channel, request, timeoutMillis)
					.thenApply(ResponseFuture::getResponseCommand)
					.get(timeoutMillis, TimeUnit.MILLISECONDS);
		}
		catch (ExecutionException e) {
			throw new RemotingSendRequestException(channel.remoteAddress().toString(), e.getCause());
		}
		catch (TimeoutException e) {
			throw new RemotingTimeoutException(channel.remoteAddress().toString(), timeoutMillis, e.getCause());
		}
	}

	protected CompletableFuture<ResponseFuture> invokeImpl(Channel channel, RemotingCommand request, long timeoutMillis) {
		return invoke0(channel, request, timeoutMillis);
	}

	protected CompletableFuture<ResponseFuture> invoke0(Channel channel, RemotingCommand request, long timeoutMillis) {
		CompletableFuture<ResponseFuture> future = new CompletableFuture<>();
		long beginStartTime = System.currentTimeMillis();
		int opaque = request.getOpaque();

		boolean acquired;
		try {
			acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
		}
		catch (Throwable e) {
			future.completeExceptionally(e);
			return future;
		}

		if (acquired) {
			SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
			long costTime = System.currentTimeMillis() - beginStartTime;
			if (timeoutMillis < costTime) {
				once.release();
				future.completeExceptionally(new RemotingTimeoutException("invokeAsyncImpl call timeout"));
				return future;
			}

			AtomicReference<ResponseFuture> responseFutureReference = new AtomicReference<>();
			ResponseFuture responseFuture = new ResponseFuture(channel, opaque, request, timeoutMillis - costTime, new InvokeCallback() {
				@Override
				public void operationComplete(ResponseFuture responseFuture) {

				}

				@Override
				public void operationSucceed(RemotingCommand response) {
					future.complete(responseFutureReference.get());
				}

				@Override
				public void operationFail(Throwable throwable) {
					future.completeExceptionally(throwable);
				}
			}, once);
			responseFutureReference.set(responseFuture);
			this.responseTable.put(opaque, responseFuture);

			try {
				channel.writeAndFlush(request).addListener(f -> {
					if (f.isSuccess()) {
						responseFuture.setSendRequestOK(true);
						return;
					}
					requestFail(opaque);
					log.warn("send a request command to channel <{}>, channelId={}, failed", RemotingHelper.parseChannelRemoteAddr(channel), channel.id());
				});
				return future;
			}
			catch (Exception e) {
				responseTable.remove(opaque);
				responseFuture.release();
				log.warn("send a request command to channel <{}> channelId={} Exception", RemotingHelper.parseChannelRemoteAddr(channel), channel.id(), e);
				future.completeExceptionally(new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e));
				return future;
			}
		}
		else {
			if (timeoutMillis <= 0) {
				future.completeExceptionally(new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast"));
			}
			else {
				String info = String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thead nums: %d semaphoreAsyncValue: %d",
						timeoutMillis, this.semaphoreAsync.getQueueLength(), this.semaphoreAsync.availablePermits());
				log.warn(info);
				future.completeExceptionally(new RemotingTimeoutException(info));
			}
		}
		return future;
	}

	public void invokeAsyncImpl(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) {
		invokeImpl(channel, request, timeoutMillis)
				.whenComplete((v, t) -> {
					if (t == null) {
						invokeCallback.operationComplete(v);
					}
					else {
						ResponseFuture responseFuture = new ResponseFuture(channel, request.getOpaque(), request, timeoutMillis, null, null);
						responseFuture.setCause(t);
						invokeCallback.operationComplete(responseFuture);
					}
				})
				.thenAccept(responseFuture -> invokeCallback.operationSucceed(responseFuture.getResponseCommand()))
				.exceptionally(t -> {
					invokeCallback.operationFail(t);
					return null;
				});
	}

	public void invokeOnewayImpl(Channel channel, RemotingCommand request, long timeoutMillis) throws RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException, InterruptedException {
		request.markOnewayRPC();
		boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
		if (acquired) {
			SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
			try {
				channel.writeAndFlush(request).addListener(f -> {
					once.release();
					if (!f.isSuccess()) {
						log.warn("send a request command to channel <{}> failed", channel.remoteAddress());
					}
				});
			}
			catch (Exception e) {
				once.release();
				log.warn("write send a request command to channel <{}> failed", channel.remoteAddress());
				throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
			}
		}
		else {
			if (timeoutMillis <= 0) {
				throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
			}
			else {
				String info = String.format("invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreOnewayValue: %d",
						timeoutMillis, this.semaphoreOneway.getQueueLength(), this.semaphoreOneway.availablePermits());
				log.warn(info);
				throw new RemotingTimeoutException(info);
			}
		}
	}

	private Runnable buildProcessRequestHandler(ChannelHandlerContext ctx, RemotingCommand cmd, Pair<NettyRequestProcessor, ExecutorService> pair, int opaque) {
		return () -> {
			Exception exception = null;
			RemotingCommand response;

			try {
				String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
				try {
					doBeforeRpcHooks(remoteAddr, cmd);
				}
				catch (AbortProcessException e) {
					throw e;
				}
				catch (Exception e) {
					exception = e;
				}

				if (this.requestPipeline != null) {
					this.requestPipeline.execute(ctx, cmd);
				}

				if (exception == null) {
					response = pair.getObject1().processRequest(ctx, cmd);
				}
				else {
					response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, null);
				}

				try {
					doAfterRpcHooks(remoteAddr, cmd, response);
				}
				catch (AbortProcessException e) {
					throw e;
				}
				catch (Exception e) {
					exception = e;
				}

				if (exception != null) {
					throw exception;
				}

				writeResponse(ctx.channel(), cmd, response);
			}
			catch (AbortProcessException e) {
				response = RemotingCommand.createResponseCommand(e.getResponseCode(), e.getErrorMessage());
				response.setOpaque(opaque);
				writeResponse(ctx.channel(), cmd, response);
			}
			catch (Throwable e) {
				log.error("process request exception", e);
				log.error(cmd.toString());
			}
		};
	}

	protected void failFast(Channel channel) {
		for (Map.Entry<Integer, ResponseFuture> entry : responseTable.entrySet()) {
			if (entry.getValue().getChannel() == channel) {
				Integer opaque = entry.getKey();
				if (opaque != null) {
					requestFail(opaque);
				}
			}
		}
	}

	private void executeInvokeCallback(ResponseFuture future) {
		boolean runInThisThread = false;
		ExecutorService executor = this.getCallbackExecutor();
		if (executor != null && !executor.isShutdown()) {
			try {
				executor.submit(() -> {
					try {
						future.executeInvokeCallback();
					}
					catch (Throwable e) {
						log.warn("execute callback in executor exception, and callback throw", e);
					}
					finally {
						future.release();
					}
				});
			}
			catch (Exception e) {
				runInThisThread = true;
				log.warn("execute callback in executor exception, maybe executor busy", e);
			}
		}
		else {
			runInThisThread = true;
		}

		if (runInThisThread) {
			try {
				future.executeInvokeCallback();
			}
			catch (Throwable e) {
				log.warn("executeInvokeCallback exception", e);
			}
			finally {
				future.release();
			}
		}
	}

	private void requestFail(int opaque) {
		ResponseFuture responseFuture = responseTable.remove(opaque);
		if (responseFuture != null) {
			responseFuture.setSendRequestOK(false);
			responseFuture.putResponse(null);
			try {
				executeInvokeCallback(responseFuture);
			}
			catch (Throwable e) {
				log.warn("execute callback in requestFail, and callback throw", e);
			}
			finally {
				responseFuture.release();
			}
		}
	}

	public List<RPCHook> getRpcHooks() {
		return rpcHooks;
	}

	public void registerRPCHook(RPCHook rpcHook) {
		if (rpcHook != null && !rpcHooks.contains(rpcHook)) {
			rpcHooks.add(rpcHook);
		}
	}

	public void clearRPCHook() {
		rpcHooks.clear();
	}

	public RequestPipeline getRequestPipeline() {
		return requestPipeline;
	}

	public void setRequestPipeline(RequestPipeline requestPipeline) {
		this.requestPipeline = requestPipeline;
	}

	class NettyEventExecutor extends ServiceThread {

		private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<>();

		public void putNettyEvent(final NettyEvent event) {
			int currentSize = this.eventQueue.size();
			int maxSize = 10000;
			if (currentSize <= maxSize) {
				this.eventQueue.add(event);
			}
			else {
				log.warn("event queue size [{}] over the limit [{}], so drop this event {}", currentSize, maxSize, event.toString());
			}
		}

		@Override
		public void run() {
			log.info(this.getServiceName() + " service started");

			final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

			while (!this.isStopped()) {
				try {
					NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
					if (event != null && listener != null) {
						switch (event.getType()) {
							case IDLE:
								listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
								break;
							case CLOSE:
								listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
								break;
							case CONNECT:
								listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
								break;
							case EXCEPTION:
								listener.onChannelException(event.getRemoteAddr(), event.getChannel());
								break;
							case ACTIVE:
								listener.onChannelActive(event.getRemoteAddr(), event.getChannel());
								break;
							default:
								break;
						}
					}
				}
				catch (Exception e) {
					log.warn(this.getServiceName() + " service has exception", e);
				}
			}

			log.info(this.getServiceName() + " service end");
		}

		@Override
		public String getServiceName() {
			return NettyEventExecutor.class.getSimpleName();
		}
	}
}
