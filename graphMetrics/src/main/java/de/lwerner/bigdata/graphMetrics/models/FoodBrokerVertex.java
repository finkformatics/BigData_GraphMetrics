package de.lwerner.bigdata.graphMetrics.models;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * Model class for FoodBrokerVertex
 * Used by jackson to read the json as a object.
 * 
 * @author Toni Pohl
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class FoodBrokerVertex {

	private long id;
	private JsonNode data;
	private JsonNode meta;
	
	public long getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public JsonNode getData() {
		return data;
	}
	
	public void setData(JsonNode data) {
		this.data = data;
	}

	public JsonNode getMeta() {
		return meta;
	}

	public void setMeta(JsonNode meta) {
		this.meta = meta;
	}
}
