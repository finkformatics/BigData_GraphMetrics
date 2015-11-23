package de.lwerner.bigdata.graphMetrics.utils;

import java.util.Comparator;
import java.util.HashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ArgumentsParser {

	public static CommandLineArguments parseArguments(String className, String... arguments) throws ParseException, IllegalArgumentException {
		Options options = new Options();
		options.addOption("m", "maxIterations", true, "max iterations on converging algorithms");
		options.addOption("n", "nodes", true, "absolute path to nodes json file");
		options.addOption("e", "edges", true, "absolute path to edges json file");
		options.addOption("o", "output", true, "absolute path to output file");
		
		CommandLineArguments args = new CommandLineArguments();
		CommandLineParser parser = new DefaultParser();
		try {
			CommandLine cli = parser.parse(options, arguments);
			if (cli.hasOption("maxIterations")) {
				try {
					args.setMaxIterations(Integer.parseInt(cli.getOptionValue("maxIterations")));
				} catch (Exception e) {
					throw new IllegalArgumentException();
				}
			}
			if (cli.hasOption("nodes")) {
				args.setNodesPath(cli.getOptionValue("nodes"));
			}
			if (cli.hasOption("edges")) {
				args.setEdgesPath(cli.getOptionValue("edges"));
			}
			if (cli.hasOption("output")) {
				args.setOutputPath(cli.getOptionValue("output"));
			}
		} catch(IllegalArgumentException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.setOptionComparator(new GraphMetricsOptionComparator());
			formatter.printHelp(100, "java " + className, "", options, "", true);
			throw e;
		} catch (ParseException e) {
			System.err.println("Parsing failed. Reason: " + e.getMessage());
			throw e;
		}
		return args;
	}
	
	public static final class GraphMetricsOptionComparator implements Comparator<Option> {

		private HashMap<String, Integer> orderMap;
		
		public GraphMetricsOptionComparator() {
			orderMap = new HashMap<>();
			orderMap.put("n", 1);
			orderMap.put("e", 2);
			orderMap.put("o", 3);
			orderMap.put("m", 4);
		}
		
		@Override
		public int compare(Option o1, Option o2) {
			return Integer.compare(orderMap.get(o1.getOpt()), orderMap.get(o2.getOpt()));
		}
		
	}
	
	public static void main(String[] args) {
		try {
			CommandLineArguments cli = parseArguments(ArgumentsParser.class.getName(), args);
			System.out.println(cli);
		} catch (ParseException | IllegalArgumentException e) {
			// Nothing to do
		}
	}
	
}