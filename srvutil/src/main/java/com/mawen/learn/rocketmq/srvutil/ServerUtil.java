package com.mawen.learn.rocketmq.srvutil;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public class ServerUtil {

	public static Options buildCommandLineOptions(final Options options) {
		Option option = new Option("h", "help", false, "Print help");
		option.setRequired(false);
		options.addOption(option);

		option = new Option("n", "namesrvAddr", true, "Name server address lit, eg: '192.168.0.1:9876;192.168.0.2:9876'");
		option.setRequired(false);
		options.addOption(option);

		return options;
	}

	public static CommandLine parseCommandLine(String appName, String[] args, Options options, CommandLineParser parser) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(110);

		CommandLine commandLine = null;
		try {
			commandLine = parser.parse(options, args);
			if (commandLine.hasOption('h')) {
				formatter.printHelp(appName, options, true);
				System.exit(0);
			}
		}
		catch (ParseException e) {
			System.err.println(e.getMessage());
			formatter.printHelp(appName, options, true);
			System.exit(1);
		}

		return commandLine;
	}

	public static void printCommandLineHelp(String appName, Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(110);
		formatter.printHelp(appName, options, true);
	}

	public static Properties commandLine2Properties(final CommandLine commandLine) {
		Properties properties = new Properties();
		Option[] options = commandLine.getOptions();

		if (options != null) {
			for (Option option : options) {
				String name = option.getLongOpt();
				String value = commandLine.getOptionValue(name);
				if (value != null) {
					properties.setProperty(name, value);
				}
			}
		}

		return properties;
	}
}
