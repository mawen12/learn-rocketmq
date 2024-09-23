package com.mawen.learn.rocketmq.common.constant;


import io.grpc.Context;
import io.grpc.Metadata;
import io.opentelemetry.sdk.metrics.internal.state.Measurement;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class GrpcConstants {

	public static final Context.Key<Metadata> METADATA = Context.key("rpc-metadata");

	public static final Metadata.Key<String> REMOTE_ADDRESS = Metadata.Key.of("rpc-remote-address", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> LOCAL_ADDRESS = Metadata.Key.of("rpc-local-address", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> AUTHORIZATION = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> NAMESPACE_ID = Metadata.Key.of("x-mq-namespace", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> DATE_TIME = Metadata.Key.of("x-mq-date-time", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> REQUEST_ID = Metadata.Key.of("x-mq-request-id", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> LANGUAGE = Metadata.Key.of("x-mq-language", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> CLIENT_VERSION = Metadata.Key.of("x-mq-client-version", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> PROTOCOL_VERSION = Metadata.Key.of("x-mq-protocol", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> RPC_NAME = Metadata.Key.of("x-mq-rpc-name", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> SIMPLE_RPC_NAME = Metadata.Key.of("x-mq-simple-rpc-name", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> SESSION_TOKEN = Metadata.Key.of("x-mq-session-token", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> CLIENT_ID = Metadata.Key.of("x-mq-client-id", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> AUTHORIZATION_AK = Metadata.Key.of("x-mq-authorization-ak", Metadata.ASCII_STRING_MARSHALLER);

	public static final Metadata.Key<String> CHANNEL_ID = Metadata.Key.of("x-mq-channel-id", Metadata.ASCII_STRING_MARSHALLER);
}
