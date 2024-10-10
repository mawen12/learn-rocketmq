package com.mawen.learn.rocketmq.common.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;


/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class FutureUtils {

	public static <T> CompletableFuture<T> addExecutor(CompletableFuture<T> future, ExecutorService executor) {
		return appendNextFuture(future, new CompletableFuture<>(), executor);
	}

	public static <T> CompletableFuture<T> appendNextFuture(CompletableFuture<T> future, CompletableFuture<T> nextFuture, ExecutorService executor) {
		future.whenCompleteAsync((t, throwable) -> {
			if (throwable != null) {
				nextFuture.completeExceptionally(throwable);
			}
			else {
				nextFuture.complete(t);
			}
		}, executor);
		return nextFuture;
	}

	public static <T> CompletableFuture<T> completeExceptionally(Throwable t) {
		CompletableFuture<T> future = new CompletableFuture<>();
		future.completeExceptionally(t);
		return future;
	}
}
