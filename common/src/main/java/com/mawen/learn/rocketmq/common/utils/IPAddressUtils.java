package com.mawen.learn.rocketmq.common.utils;

import java.math.BigInteger;
import java.net.InetAddress;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.InetAddressValidator;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class IPAddressUtils {

	private static final String SLASH = "/";

	private static final InetAddressValidator VALIDATOR = InetAddressValidator.getInstance();

	public static boolean isValidIpOrCidr(String ipOrCidr) {
		return isValidIp(ipOrCidr) || isValidCidr(ipOrCidr);
	}

	public static boolean isValidIp(String ip) {
		return VALIDATOR.isValid(ip);
	}

	public static boolean isValidCidr(String cidr) {
		return isValidIPv4Cidr(cidr) || isValidIPv6Cidr(cidr);
	}

	public static boolean isValidIPv4Cidr(String cidr) {
		try {
			String[] parts = cidr.split(SLASH);
			if (parts.length != 2) {
				return false;
			}

			InetAddress ip = InetAddress.getByName(parts[0]);
			if (ip.getAddress().length != 4) {
				return false;
			}

			int prefix = Integer.parseInt(parts[1]);
			return prefix >= 0 && prefix <= 32;
		}
		catch (Exception e) {
			return false;
		}
	}

	public static boolean isValidIPv6Cidr(String cidr) {
		try {
			String[] parts = cidr.split(SLASH);
			if (parts.length != 2) {
				return false;
			}

			InetAddress ip = InetAddress.getByName(parts[0]);
			if (ip.getAddress().length != 16) {
				return false;
			}

			int prefix = Integer.parseInt(parts[1]);
			return prefix >= 0 && prefix <= 127;
		}
		catch (Exception e) {
			return false;
		}
	}

	public static boolean isIPInRange(String ip, String cidr) {
		try {
			String[] parts = cidr.split(SLASH);
			if (parts.length == 1) {
				return StringUtils.equals(ip, cidr);
			}

			if (parts.length != 2) {
				return false;
			}

			InetAddress cidrIp = InetAddress.getByName(parts[0]);
			int prefixLength = Integer.parseInt(parts[1]);

			BigInteger cidrIpBigInt = new BigInteger(1, cidrIp.getAddress());
			BigInteger ipBigInt = new BigInteger(1, InetAddress.getByName(ip).getAddress());

			BigInteger mask = BigInteger.valueOf(-1).shiftLeft(cidrIp.getAddress().length * 8 - prefixLength);
			BigInteger cidrIpLower = cidrIpBigInt.and(mask);
			BigInteger cidrIpUpper = cidrIpLower.add(mask.not());

			return ipBigInt.compareTo(cidrIpLower) >= 0 && ipBigInt.compareTo(cidrIpUpper) <= 0;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
