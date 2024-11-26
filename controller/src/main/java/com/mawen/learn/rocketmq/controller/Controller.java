package com.mawen.learn.rocketmq.controller;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.mawen.learn.rocketmq.controller.helper.BrokerLifecycleListener;
import com.mawen.learn.rocketmq.remoting.RemotingServer;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.body.SyncStateSet;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
public interface Controller {

	void startup();

	void shutdown();

	void startScheduling();

	void stopScheduling();

	void isLeaderState();

	CompletableFuture<RemotingCommand> alterSyncStateSet(final AlterSyncStateSetRequestHeader request, final SyncStateSet syncStateSet);

	CompletableFuture<RemotingCommand> electMaster(final ElectMasterRequestHeader request);

	CompletableFuture<RemotingCommand> getNextBrokerId(final GetNextBrokerIdRequestHeader request);

	CompletableFuture<RemotingCommand> applyBrokerId(final ApplyBrokerIdRequestHeader request);

	CompletableFuture<RemotingCommand> registerBroker(final RegisterBrokerToControllerRequestHeader request);

	CompletableFuture<RemotingCommand> getReplicaInfo(final GetReplicaInfoRequestHeader request);

	RemotingCommand getControllerMetadata();

	CompletableFuture<RemotingCommand> getSyncStateData(final List<String> brokerNames);

	void registerBrokerLifecycleListener(final BrokerLifecycleListener listener);

	RemotingServer getRemotingServer();

	CompletableFuture<RemotingCommand> cleanBrokerData(final CleanControllerBrokerDataRequestHeader request);
}

