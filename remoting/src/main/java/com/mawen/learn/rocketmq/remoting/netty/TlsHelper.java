package com.mawen.learn.rocketmq.remoting.netty;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.util.Properties;

import com.google.common.base.Strings;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import static com.mawen.learn.rocketmq.remoting.netty.TlsSystemConfig.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
public class TlsHelper {

	private static final Logger log = LoggerFactory.getLogger(TlsHelper.class);

	private static DecryptionStrategy decryptionStrategy = new DecryptionStrategy() {
		@Override
		public InputStream decryptPrivateKey(String privateKeyEncryptPath, boolean forClient) throws IOException {
			return new FileInputStream(privateKeyEncryptPath);
		}
	};

	public static void registerDecryptionStrategy(final DecryptionStrategy decryptionStrategy) {
		TlsHelper.decryptionStrategy = decryptionStrategy;
	}

	public static SslContext buildSslContext(boolean forClient) throws IOException, CertificateException{
		File configFile = new File(tlsConfigFile);
		extractTlsConfigFromFile(configFile);
		logTheFinalUsedTlsConfig();

		SslProvider sslProvider;
		if (OpenSsl.isAvailable()) {
			sslProvider = SslProvider.OPENSSL;
			log.info("Using OpenSSl provider");
		}
		else {
			sslProvider = SslProvider.JDK;
			log.info("Using JDK SSL provider");
		}

		if (forClient) {
			if (tlsTestModeEnable) {
				return SslContextBuilder
						.forClient()
						.sslProvider(SslProvider.JDK)
						.trustManager(InsecureTrustManagerFactory.INSTANCE)
						.build();
			}
			else {
				SslContextBuilder builder = SslContextBuilder.forClient().sslProvider(SslProvider.JDK);

				if (!tlsClientAuthServer) {
					builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
				}
				else {
					if (!Strings.isNullOrEmpty(tlsClientTrustCertPath)) {
						builder.trustManager(new File(tlsClientTrustCertPath));
					}
				}

				return builder.keyManager(
								!Strings.isNullOrEmpty(tlsClientCertPath) ? new FileInputStream(tlsClientCertPath) : null,
								!Strings.isNullOrEmpty(tlsClientKeyPath) ? decryptionStrategy.decryptPrivateKey(tlsClientKeyPath, true) : null,
								!Strings.isNullOrEmpty(tlsClientKeyPassword) ? tlsClientKeyPassword : null)
						.build();
			}
		}
		else {
			if (tlsTestModeEnable) {
				SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
				return SslContextBuilder
						.forServer(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
						.sslProvider(sslProvider)
						.clientAuth(ClientAuth.OPTIONAL)
						.build();
			}
			else {
				SslContextBuilder builder = SslContextBuilder
						.forServer(
								!Strings.isNullOrEmpty(tlsServerCertPath) ? new FileInputStream(tlsServerCertPath) : null,
								!Strings.isNullOrEmpty(tlsServerKeyPath) ? decryptionStrategy.decryptPrivateKey(tlsServerKeyPath, false) : null,
								!Strings.isNullOrEmpty(tlsServerKeyPassword) ? tlsServerKeyPassword : null)
						.sslProvider(sslProvider);

				if (!tlsServerAuthClient) {
					builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
				}
				else {
					if (!Strings.isNullOrEmpty(tlsServerTrustCertPath)) {
						builder.trustManager(new File(tlsServerCertPath));
					}
				}

				return builder.clientAuth(parseClientAuthMode(tlsServerNeedClientAuth))
						.build();
			}
		}
	}

	private static void extractTlsConfigFromFile(File configFile) {
		if (!(configFile.exists() && configFile.isFile() && configFile.canRead())) {
			log.info("Tls config file doesn't exist, skip it");
			return;
		}

		Properties properties = new Properties();
		try (InputStream inputStream = new FileInputStream(configFile)) {
			properties.load(inputStream);
		}
		catch (IOException ignored) {
		}

		tlsTestModeEnable = Boolean.parseBoolean(properties.getProperty(TLS_TEST_MODE_ENABLE, String.valueOf(tlsTestModeEnable)));
		tlsServerNeedClientAuth = properties.getProperty(TLS_SERVER_NEED_CLIENT_AUTH, tlsServerNeedClientAuth);
		tlsServerKeyPath = properties.getProperty(TLS_SERVER_KEYPATH, tlsServerKeyPath);
		tlsServerKeyPassword = properties.getProperty(TLS_SERVER_KEYPASSWORD, tlsServerKeyPassword);
		tlsServerCertPath = properties.getProperty(TLS_SERVER_CERTPATH, tlsServerCertPath);
		tlsServerAuthClient = Boolean.parseBoolean(System.getProperty(TLS_SERVER_AUTHCLIENT, String.valueOf(tlsServerAuthClient)));
		tlsServerTrustCertPath = properties.getProperty(TLS_SERVER_TRUSTCERTPATH, tlsServerTrustCertPath);

		tlsClientKeyPath = properties.getProperty(TLS_CLIENT_KEYPATH, tlsClientKeyPath);
		tlsClientKeyPassword = properties.getProperty(TLS_CLIENT_KEYPASSWORD, tlsClientKeyPassword);
		tlsClientCertPath = properties.getProperty(TLS_CLIENT_CERTPATH, tlsClientCertPath);
		tlsClientAuthServer = Boolean.parseBoolean(properties.getProperty(TLS_CLIENT_AUTHSERVER, String.valueOf(tlsClientAuthServer)));
		tlsClientTrustCertPath = properties.getProperty(TLS_CLIENT_TRUSTCERTPATH, tlsClientTrustCertPath);
	}

	private static void logTheFinalUsedTlsConfig() {
		log.info("Log the final used tls related configuration");
		log.info("{} = {}", TLS_TEST_MODE_ENABLE, tlsTestModeEnable);
		log.info("{} = {}", TLS_SERVER_NEED_CLIENT_AUTH, tlsServerNeedClientAuth);
		log.info("{} = {}", TLS_SERVER_KEYPATH, tlsServerKeyPath);
		log.info("{} = {}", TLS_SERVER_KEYPASSWORD, tlsServerKeyPassword);
		log.info("{} = {}", TLS_SERVER_CERTPATH, tlsServerCertPath);
		log.info("{} = {}", TLS_SERVER_AUTHCLIENT, tlsServerAuthClient);
		log.info("{} = {}", TLS_CLIENT_KEYPATH, tlsClientKeyPath);
		log.info("{} = {}", TLS_CLIENT_KEYPASSWORD, tlsClientKeyPassword);
		log.info("{} = {}", TLS_CLIENT_CERTPATH, tlsClientCertPath);
		log.info("{} = {}", TLS_CLIENT_AUTHSERVER, tlsClientAuthServer);
		log.info("{} = {}", TLS_CLIENT_TRUSTCERTPATH, tlsClientTrustCertPath);
	}

	private static ClientAuth parseClientAuthMode(String authMode) {
		if (StringUtils.isBlank(authMode)) {
			return ClientAuth.NONE;
		}

		authMode = authMode.toUpperCase();

		for (ClientAuth clientAuth : ClientAuth.values()) {
			if (clientAuth.name().equals(authMode)) {
				return clientAuth;
			}
		}
		return ClientAuth.NONE;
	}

	public interface DecryptionStrategy {

		InputStream decryptPrivateKey(String privateKeyEncryptPath, boolean forClient) throws IOException;
	}

}
