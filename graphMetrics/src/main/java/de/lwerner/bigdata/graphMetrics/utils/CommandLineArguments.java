package de.lwerner.bigdata.graphMetrics.utils;

public class CommandLineArguments {

	private int maxIterations;
	private String nodesPath;
	private String edgesPath;
	private String outputPath;

	public CommandLineArguments() {
		maxIterations = 10;
		nodesPath = getClass().getResource("/nodes.json").getPath();
		edgesPath = getClass().getResource("/edges.json").getPath();
	}

	public CommandLineArguments(int maxIterations, String nodesPath, String edgesPath, String outputPath) {
		this.maxIterations = maxIterations;
		this.nodesPath = nodesPath;
		this.edgesPath = edgesPath;
		this.outputPath = outputPath;
	}

	public int getMaxIterations() {
		return maxIterations;
	}

	public void setMaxIterations(int maxIterations) {
		this.maxIterations = maxIterations;
	}

	public String getNodesPath() {
		return nodesPath;
	}

	public void setNodesPath(String nodesPath) {
		this.nodesPath = nodesPath;
	}
	
	public String getEdgesPath() {
		return edgesPath;
	}
	
	public void setEdgesPath(String edgesPath) {
		this.edgesPath = edgesPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	@Override
	public String toString() {
		return "CommandLineArguments [maxIterations=" + maxIterations + ", nodesPath=" + nodesPath + ", edgesPath=" + edgesPath + ", outputPath="
				+ outputPath + "]";
	}

}