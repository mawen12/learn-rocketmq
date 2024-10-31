package com.mawen.learn.rocketmq.client.impl.mqclient;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Strings;
import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.client.common.NameserverAccessConfig;
import com.mawen.learn.rocketmq.client.impl.ClientRemotingProcessor;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.utils.StartAndShutdown;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/28
 */
public class MQClientAPIFactory implements StartAndShutdown {

	private MQClientAPIExt[] clients;
	private final String namePrefix;
	private final int clientNum;
	private final ClientRemotingProcessor clientRemotingProcessor;
	private final RPCHook rpcHook;
	private final ScheduledExecutorService scheduledExecutorService;
	private final NameserverAccessConfig nameserverAccessConfig;

	public MQClientAPIFactory(NameserverAccessConfig nameserverAccessConfig, String namePrefix, int clientNum, ClientRemotingProcessor clientRemotingProcessor, RPCHook rpcHook, ScheduledExecutorService scheduledExecutorService) {
		this.nameserverAccessConfig = nameserverAccessConfig;
		this.namePrefix = namePrefix;
		this.clientNum = clientNum;
		this.clientRemotingProcessor = clientRemotingProcessor;
		this.rpcHook = rpcHook;
		this.scheduledExecutorService = scheduledExecutorService;

		this.init();
	}

	@Override
	public void start() throws Exception {
		this.clients = new MQClientAPIExt[this.clientNum];

		for (int i = 0; i < this.clientNum; i++) {
			clients[i] = createAndStart(this.namePrefix + "N_" + i);
		}
	}

	@Override
	public void shutdown() throws Exception {
		for (int i = 0; i < this.clientNum; i++) {
			clients[i].shutdown();
		}
	}

	public MQClientAPIExt getClient() {
		if (clients.length == 1) {
			return this.clients[0];
		}

		int index = ThreadLocalRandom.current().nextInt(this.clients.length);
		return this.clients[index];
	}

	protected void init() {
		System.setProperty(ClientConfig.SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false");
		if (StringUtils.isEmpty(nameserverAccessConfig.getNamesrvDomain())) {
			if (Strings.isNullOrEmpty(nameserverAccessConfig.getNamesrvAddr())) {
				throw new RuntimeException("The configuration item NamesrvAddr is not configured");
			}
			System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, nameserverAccessConfig.getNamesrvAddr());
		}
		else {
			System.setProperty("rocketmq.namesrv.domain", nameserverAccessConfig.getNamesrvDomain());
			System.setProperty("rocketmq.namesrv.domain.subgroup", nameserverAccessConfig.getNamesrvDomainSubgroup());
		}
	}

	protected MQClientAPIExt createAndStart(String instanceName) {
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.setInstanceName(instanceName);
		clientConfig.setDecodeReadBody(true);
		clientConfig.setDecodeDecompressBody(false);

		NettyClientConfig nettyClientConfig = new NettyClientConfig();
		nettyClientConfig.setDisableCallbackExecutor(true);

		MQClientAPIExt mqClientAPIExt = new MQClientAPIExt(clientConfig, nettyClientConfig, clientRemotingProcessor, rpcHook);
		if (!mqClientAPIExt.updateNameServerAddressList()) {
			mqClientAPIExt.fetchNameServerAddr();
			this.scheduledExecutorService.scheduleAtFixedRate(mqClientAPIExt::fetchNameServerAddr, Duration.ofSeconds(10).toMillis(), Duration.ofMinutes(2).toMillis(), TimeUnit.MILLISECONDS);
		}
		mqClientAPIExt.start();

		return mqClientAPIExt;
	}
}
