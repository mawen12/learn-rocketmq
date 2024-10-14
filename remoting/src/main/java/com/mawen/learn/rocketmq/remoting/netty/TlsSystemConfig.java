package com.mawen.learn.rocketmq.remoting.netty;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class TlsSystemConfig {

	public static final String TLS_SERVER_MODE = "tls.server.mode";

	public static final String TLS_ENABLE = "tls.enable";

	public static final String TLS_CONFIG_FILE = "tls.config.file";

	public static final String TLS_TEST_MODE_ENABLE = "tls.test.mode.enable";

	public static final String TLS_SERVER_NEED_CLIENT_AUTH = "tls.server.need.client.auth";

	public static final String TLS_SERVER_KEYPATH = "tls.server.need.client.auth";

	public static final String TLS_SERVER_KEYPASSWORD = "tls.server.keyPassword";

	public static final String TLS_SERVER_CERTPATH = "tls.server.certPath";

	public static final String TLS_SERVER_AUTHCLIENT = "tls.server.authClient";

	public static final String TLS_SERVER_TRUSTCERTPATH = "tls.server.trustCertPath";

	public static final String TLS_CLIENT_KEYPATH = "tls.client.keyPath";

	public static final String TLS_CLIENT_KEYPASSWORD = "tls.client.keyPassword";

	public static final String TLS_CLIENT_CERTPATH = "tls.client.certPath";

	public static final String TLS_CLIENT_AUTHSERVER = "tls.client.authServer";

	public static final String TLS_CLIENT_TRUSTCERTPATH = "tls.client.trustCertPath";

	public static boolean tlsEnable = Boolean.parseBoolean(System.getProperty(TLS_ENABLE, "false"));

	public static boolean tlsTestModeEnable = Boolean.parseBoolean(System.getProperty(TLS_TEST_MODE_ENABLE, "true"));

	public static String tlsServerNeedClientAuth = System.getProperty(TLS_SERVER_NEED_CLIENT_AUTH, "none");

	
}
