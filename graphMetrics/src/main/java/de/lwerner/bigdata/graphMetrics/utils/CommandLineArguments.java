package de.lwerner.bigdata.graphMetrics.utils;

public class CommandLineArguments {

	private int maxIterations;
	private String inputPath;
	private String outputPath;

	public CommandLineArguments() {
		// Empty constructor
	}

	public CommandLineArguments(int maxIterations, String inputPath, String outputPath) {
		this.maxIterations = maxIterations;
		this.inputPath = inputPath;
		this.outputPath = outputPath;
	}

	public int getMaxIterations() {
		return maxIterations;
	}

	public void setMaxIterations(int maxIterations) {
		this.maxIterations = maxIterations;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	@Override
	public String toString() {
		return "CommandLineArguments [maxIterations=" + maxIterations + ", inputPath=" + inputPath + ", outputPath="
				+ outputPath + "]";
	}

}