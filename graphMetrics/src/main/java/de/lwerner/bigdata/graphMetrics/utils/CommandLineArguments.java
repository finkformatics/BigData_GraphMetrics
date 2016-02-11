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
	 * GraphReader type simple|food
	 */
	private String reader;
	/**
	 * Path to the vertices file
	 */
	private String verticesPath;
	/**
	 * Path to the edges file
	 */
	private String edgesPath;
	/**
	 * Path to the graph file
	 */
	private String graphPath;
	/**
	 * Path for output file
	 */
	private String outputPath;

	/**
	 * Sets default values
	 */
	public CommandLineArguments() {
		maxIterations = 10;
		reader = "food";
		verticesPath = getClass().getResource("/vertices.json").getPath();
		edgesPath = getClass().getResource("/edges.json").getPath();
		graphPath = getClass().getResource("/Email-EuAll.txt").getPath();
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
	 * @return the reader type
     */
	public String getReader() {
		return reader;
	}

	/**
	 * @param reader the reader type to set
     */
	public void setReader(String reader) {
		this.reader = reader;
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
	 * @return the graph path
     */
	public String getGraphPath() {
		return graphPath;
	}

	/**
	 * @param graphPath the graph path to set
     */
	public void setGraphPath(String graphPath) {
		this.graphPath = graphPath;
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