package access.database.model;

/**
 * JSON Database Model, serialized by Jackson, that represents a Lease in the
 * Piazza System.
 * 
 * @author Patrick.Doody
 * 
 */
public class Lease {
	public String id;
	public String deploymentId;
	public String expirationDate;

	public Lease() {

	}
}
