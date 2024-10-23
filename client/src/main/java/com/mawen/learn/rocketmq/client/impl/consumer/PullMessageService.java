package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.message.MessageRequestMode;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import lombok.RequiredArgsConstructor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
@RequiredArgsConstructor
public class PullMessageService extends ServiceThread {

	private static final Logger log = LoggerFactory.getLogger(PullMessageService.class);

	private final MQClientInstance mqClientFactory;
	private final LinkedBlockingQueue<MessageRequest> messageRequestQueue = new LinkedBlockingQueue<>();
	private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("PullMessageServiceScheduledThread"));

	@Override
	public String getServiceName() {
		return PullMessageService.class.getSimpleName();
	}

	@Override
	public void run() {
		log.info("{} service start", this.getServiceName());

		while (!this.isStopped()) {
			try {
				MessageRequest messageRequest = this.messageRequestQueue.take();
				if (messageRequest.getMessageRequestMode() == MessageRequestMode.POP) {
					this.popMessage((PopRequest) messageRequest);
				}
				else {
					this.pullMessage((PullRequest) messageRequest);
				}
			}
			catch (InterruptedException ignored){}
			catch (Exception e) {
				log.error("Pull Message Service Run method exception", e);
			}
		}

		log.info("{} service end", this.getServiceName());
	}

	public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
		if (!isStopped()) {
			this.scheduledExecutorService.schedule(() -> PullMessageService.this.executePullRequestImmediately(pullRequest), timeDelay, TimeUnit.MILLISECONDS);
		}
		else {
			log.warn("PullMessageServiceScheduledThread has shutdown");
		}
	}

	public void executePullRequestImmediately(final PullRequest pullRequest) {
		try {
			this.messageRequestQueue.put(pullRequest);
		}
		catch (InterruptedException e) {
			log.error("executePullRequestImmediately pullRequestQueue.put", e);
		}
	}

	public void executePopPullRequestLater(final PopRequest popRequest, final long timeDelay) {
		if (!isStopped()) {
			this.scheduledExecutorService.schedule(() -> PullMessageService.this.executePopPullRequestImmediately(popRequest), timeDelay, TimeUnit.MILLISECONDS);
		}
		else {
			log.error("PullMessageServiceScheduledThread has shutdown");
		}
	}

	public void executePopPullRequestImmediately(final PopRequest popRequest) {
		try {
			this.messageRequestQueue.put(popRequest);
		}
		catch (InterruptedException e) {
			log.error("executePopPullRequestImmediately pullRequestQueue.put exception", e);
		}
	}

	public void executeTaskLater(final Runnable r, final long timeDelay) {
		if (!isStopped()) {
			this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
		}
		else {
			log.warn("PullMessageServiceScheduledThread has shutdown");
		}
	}

	public void executeTask(final Runnable r) {
		if (!isStopped()) {
			this.scheduledExecutorService.execute(r);
		}else {
			log.warn("PullMessageServiceScheduledThread has shutdown");
		}
	}

	@Override
	public void shutdown(boolean interrupt) {
		super.shutdown(interrupt);
		ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
	}

	private void popMessage(PopRequest popRequest) {
		MQConsumerInner consumer = this.mqClientFactory.selectConsumer(popRequest.getConsumerGroup());
		if (consumer != null) {
			DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
			impl.popMessage(popRequest);
		}

		log.warn("No matched consumer for the PopRequest {}, drop it", popRequest);
	}

	private void pullMessage(PullRequest pullRequest) {
		 MQConsumerInner consumer = this.mqClientFactory.selectConsumer(pullRequest.getConsumerGroup());
		if (consumer != null) {
			DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
			impl.pullMessage(pullRequest);
		}
		log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
	}
}
