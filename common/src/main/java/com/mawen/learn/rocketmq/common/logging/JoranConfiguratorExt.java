package com.mawen.learn.rocketmq.common.logging;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;

import com.google.common.io.CharStreams;
import org.apache.rocketmq.logging.ch.qos.logback.classic.joran.JoranConfigurator;
import org.apache.rocketmq.logging.ch.qos.logback.core.joran.spi.JoranException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public class JoranConfiguratorExt extends JoranConfigurator {

	private InputStream transformXml(InputStream in) throws IOException {
		try {
			String str = CharStreams.toString(new InputStreamReader(in, StandardCharsets.UTF_8));
			str = str.replace("\"ch.qos.logback", "\"org.apache.rocketmq.logging.ch.qos.logback");
			return new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
		}
		finally {
			if (in != null) {
				in.close();
			}
		}
	}

	public final void doConfigure0(URL url) throws JoranException {
		InputStream in = null;
		try {
			informContextOfURLUsedForConfiguration(getContext(), url);
			URLConnection urlConnection = url.openConnection();
			urlConnection.setUseCaches(false);

			InputStream temp = urlConnection.getInputStream();
			in = transformXml(temp);

			doConfigure(in, url.toExternalForm());
		}
		catch (IOException e) {
			String errMsg = "Could not open URL [" + url + "]";
			addError(errMsg, e);
			throw new JoranException(errMsg, e);
		}
		finally {
			if (in != null) {
				try {
					in.close();
				}
				catch (IOException e) {
					String errMsg = "Could not close input stream";
					addError(errMsg, e);
					throw new JoranException(errMsg, e);
				}
			}
		}
	}
}