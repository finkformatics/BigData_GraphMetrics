package de.lwerner.bigdata.graphMetrics.io;

import java.io.File;
import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

/**
 * Output class to write JSON files from given json nodes
 */
public abstract class GraphMetricsWriter {

	/**
	 * Write the json node to a given output file
	 *
	 * @param mapper Jackson ObjectMapper as helper
	 * @param jsonNode json node
	 * @param outputPath path to output file
	 * @throws IOException if the write operation fails
     */
	public static void writeJson(ObjectMapper mapper, JsonNode jsonNode, String outputPath) throws IOException {
		ObjectWriter writer = mapper.defaultPrettyPrintingWriter();
		writer.writeValue(new File(outputPath), jsonNode);
		String jsonString = writer.writeValueAsString(jsonNode);
		System.out.println(jsonString);
	}
	
}