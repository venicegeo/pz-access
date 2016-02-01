package access.database.model;

/**
 * JSON Database Model, serialized by Jackson, that represents a Deployment in
 * the Piazza System.
 * 
 * @author Patrick.Doody
 * 
 */
public class Deployment {
	public String id;
	public String resourceId;
	public String host;
	public String port;
	public String layer;
	public String capabilitiesUrl;

	public Deployment() {
	}
}
