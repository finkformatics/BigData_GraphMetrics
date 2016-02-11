package de.lwerner.bigdata.graphMetrics.io;

import java.io.File;
import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

public abstract class GraphMetricsWriter {

	public static void writeJson(ObjectMapper mapper, JsonNode jsonNode, String outputPath) throws JsonGenerationException, JsonMappingException, IOException {
		ObjectWriter writer = mapper.defaultPrettyPrintingWriter();
		writer.writeValue(new File(outputPath), jsonNode);
		String jsonString = writer.writeValueAsString(jsonNode);
		System.out.println(jsonString);
	}
	
}