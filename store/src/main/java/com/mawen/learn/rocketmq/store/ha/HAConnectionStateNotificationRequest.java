package com.mawen.learn.rocketmq.store.ha;

import java.util.concurrent.CompletableFuture;

import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/11
 */
@Getter
public class HAConnectionStateNotificationRequest {
	private final CompletableFuture<Boolean> requestFuture = new CompletableFuture<>();
	private final HAConnectionState expectState;
	private final String remoteAddr;
	private final boolean notifyWhenShutdown;

	public HAConnectionStateNotificationRequest(HAConnectionState expectState, String remoteAddr, boolean notifyWhenShutdown) {
		this.expectState = expectState;
		this.remoteAddr = remoteAddr;
		this.notifyWhenShutdown = notifyWhenShutdown;
	}
}
