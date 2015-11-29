package de.lwerner.bigdata.graphMetrics.utils;

/**
 * Holds the command line arguments in proper formats
 * 
 * @author Lukas Werner
 */
public class CommandLineArguments {

	/**
	 * Maximum iterations for converging algorithms such as DeltaIteration algorithms
	 */
	private int maxIterations;
	/**
	 * Path to the vertices file
	 */
	private String verticesPath;
	/**
	 * Path to the edges file
	 */
	private String edgesPath;
	/**
	 * Path for output file
	 */
	private String outputPath;

	/**
	 * Sets default values
	 */
	public CommandLineArguments() {
		maxIterations = 10;
		verticesPath = getClass().getResource("/vertices.json").getPath();
		edgesPath = getClass().getResource("/edges.json").getPath();
	}

	/**
	 * Takes the given arguments and sets them
	 * 
	 * @param maxIterations maximum iterations
	 * @param verticesPath path to vertices file
	 * @param edgesPath path to edges file
	 * @param outputPath path to output file
	 */
	public CommandLineArguments(int maxIterations, String verticesPath, String edgesPath, String outputPath) {
		this.maxIterations = maxIterations;
		this.verticesPath = verticesPath;
		this.edgesPath = edgesPath;
		this.outputPath = outputPath;
	}

	/**
	 * @return the max iterations
	 */
	public int getMaxIterations() {
		return maxIterations;
	}

	/**
	 * @param maxIterations the max iterations to set
	 */
	public void setMaxIterations(int maxIterations) {
		this.maxIterations = maxIterations;
	}

	/**
	 * @return the vertices path
	 */
	public String getVerticesPath() {
		return verticesPath;
	}

	/**
	 * @param verticesPath the vertices path to set
	 */
	public void setVerticesPath(String verticesPath) {
		this.verticesPath = verticesPath;
	}
	
	/**
	 * @return the edges path
	 */
	public String getEdgesPath() {
		return edgesPath;
	}
	
	/**
	 * @param edgesPath the edges path to set
	 */
	public void setEdgesPath(String edgesPath) {
		this.edgesPath = edgesPath;
	}

	/**
	 * @return the output path
	 */
	public String getOutputPath() {
		return outputPath;
	}

	/**
	 * @param outputPath the output path to set
	 */
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

}