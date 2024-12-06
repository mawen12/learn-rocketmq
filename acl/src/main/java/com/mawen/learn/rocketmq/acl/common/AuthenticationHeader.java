package com.mawen.learn.rocketmq.acl.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/2
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
public class AuthenticationHeader {

	private String remoteAddress;
	private String tenantId;
	private String namespace;
	private String authorization;
	private String datetime;
	private String sessionToken;
	private String requestId;
	private String language;
	private String clientVersion;
	private String protocol;
	private int requestCode;

	public static class MetadataHeaderBuilder {
		private String remoteAddress;
		private String tenantId;
		private String namespace;
		private String authorization;
		private String datetime;
		private String sessionToken;
		private String requestId;
		private String language;
		private String clientVersion;
		private String protocol;
		private int requestCode;

		public MetadataHeaderBuilder() {
		}

		public AuthenticationHeader.MetadataHeaderBuilder remoteAddress(final String remoteAddress) {
			this.remoteAddress = remoteAddress;
			return this;
		}

		public AuthenticationHeader.MetadataHeaderBuilder tenantId(final String tenantId) {
			this.tenantId = tenantId;
			return this;
		}

		public AuthenticationHeader.MetadataHeaderBuilder namespace(final String namespace) {
			this.namespace = namespace;
			return this;
		}

		public AuthenticationHeader.MetadataHeaderBuilder authorization(final String authorization) {
			this.authorization = authorization;
			return this;
		}

		public AuthenticationHeader.MetadataHeaderBuilder datetime(final String datetime) {
			this.datetime = datetime;
			return this;
		}

		public AuthenticationHeader.MetadataHeaderBuilder sessionToken(final String sessionToken) {
			this.sessionToken = sessionToken;
			return this;
		}

		public AuthenticationHeader.MetadataHeaderBuilder requestId(final String requestId) {
			this.requestId = requestId;
			return this;
		}

		public AuthenticationHeader.MetadataHeaderBuilder language(final String language) {
			this.language = language;
			return this;
		}

		public AuthenticationHeader.MetadataHeaderBuilder clientVersion(final String clientVersion) {
			this.clientVersion = clientVersion;
			return this;
		}

		public AuthenticationHeader.MetadataHeaderBuilder protocol(final String protocol) {
			this.protocol = protocol;
			return this;
		}

		public AuthenticationHeader.MetadataHeaderBuilder requestCode(final int requestCode) {
			this.requestCode = requestCode;
			return this;
		}

		public AuthenticationHeader build() {
			return new AuthenticationHeader(this.remoteAddress, this.tenantId, this.namespace, this.authorization,
					this.datetime, this.sessionToken, this.requestId, this.language, this.clientVersion, this.protocol,
					this.requestCode);
		}
	}

	public static AuthenticationHeader.MetadataHeaderBuilder builder() {
		return new AuthenticationHeader.MetadataHeaderBuilder();
	}
}
