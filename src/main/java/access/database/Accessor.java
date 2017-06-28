/**
 * Copyright 2016, RadiantBlue Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package access.database;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.geotools.data.DataStore;
import org.mongojack.DBCursor;
import org.mongojack.DBQuery;
import org.mongojack.DBSort;
import org.mongojack.DBUpdate;
import org.mongojack.JacksonDBCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;

import model.data.DataResource;
import model.data.deployment.Deployment;
import model.data.deployment.DeploymentGroup;
import model.data.deployment.Lease;
import model.response.DataResourceListResponse;
import model.response.DeploymentListResponse;
import model.response.Pagination;
import util.GeoToolsUtil;

/**
 * Handles Mongo access for the Deployer and the Leaser, and for the Resource collection which stores the Ingested
 * Resource metadata.
 * 
 * Also abstracts out some PostGIS accessor methods.
 * 
 * Deployments and leases have their own collections, and are managed by this Access component.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class Accessor {
	@Value("${vcap.services.pz-mongodb.credentials.uri}")
	private String DATABASE_URI;
	@Value("${vcap.services.pz-mongodb.credentials.database}")
	private String DATABASE_NAME;
	@Value("${vcap.services.pz-mongodb.credentials.host}")
	private String DATABASE_HOST;
	@Value("${vcap.services.pz-mongodb.credentials.port}")
	private int DATABASE_PORT;
	@Value("${vcap.services.pz-mongodb.credentials.username:}")
	private String DATABASE_USERNAME;
	@Value("${vcap.services.pz-mongodb.credentials.password:}")
	private String DATABASE_CREDENTIAL;
	@Value("${mongo.db.collection.resources}")
	private String resourceCollectionName;
	@Value("${mongo.db.collection.deployments}")
	private String deploymentCollectionName;
	@Value("${mongo.db.collection.deployment.groups}")
	private String deploymentGroupCollectionName;
	@Value("${mongo.db.collection.leases}")
	private String leaseCollectionName;
	@Value("${mongo.thread.multiplier}")
	private int mongoThreadMultiplier;

	@Autowired
	private Environment environment;
	
	private MongoClient mongoClient;

	private static final String DATA_ID = "dataId";
	private static final String DEPLOYMENT_ID = "deploymentId";
	private static final String DEPLOYMENTGROUP_ID = "deploymentGroupId";
	private static final String LEASE_ID = "leaseId";
	private static final String INSTANCE_NOT_AVAILABLE_ERROR = "MongoDB instance not available.";
	private static final Logger LOGGER = LoggerFactory.getLogger(Accessor.class);

	@PostConstruct
	private void initialize() {
		try {
			MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
			// Enable SSL if the `mongossl` Profile is enabled
			if (Arrays.stream(environment.getActiveProfiles()).anyMatch(env -> "mongossl".equalsIgnoreCase(env))) {
				builder.sslEnabled(true);
				builder.sslInvalidHostNameAllowed(true);
			}
			// If a username and password are provided, then associate these credentials with the connection
			if ((!StringUtils.isEmpty(DATABASE_USERNAME)) && (!StringUtils.isEmpty(DATABASE_CREDENTIAL))) {
				mongoClient = new MongoClient(new ServerAddress(DATABASE_HOST, DATABASE_PORT),
						Arrays.asList(
								MongoCredential.createCredential(DATABASE_USERNAME, DATABASE_NAME, DATABASE_CREDENTIAL.toCharArray())),
						builder.threadsAllowedToBlockForConnectionMultiplier(mongoThreadMultiplier).build());
			} else {
				mongoClient = new MongoClient(new ServerAddress(DATABASE_HOST, DATABASE_PORT),
						builder.threadsAllowedToBlockForConnectionMultiplier(mongoThreadMultiplier).build());
			}

		} catch (Exception exception) {
			LOGGER.error(String.format("Error connecting to MongoDB Instance. %s", exception.getMessage()), exception);

		}
	}

	@PreDestroy
	private void close() {
		mongoClient.close();
	}

	/**
	 * Gets a reference to the MongoDB Client Object.
	 * 
	 * @return
	 */
	public MongoClient getClient() {
		return mongoClient;
	}

	/**
	 * Gets the PostGIS data store for GeoTools.
	 * 
	 * @return Data Store.
	 */
	public DataStore getPostGisDataStore(String host, String port, String schema, String dbName, String user, String password)
			throws IOException {
		return GeoToolsUtil.getPostGisDataStore(host, port, schema, dbName, user, password);
	}

	/**
	 * Gets the Deployment for the specified Resource Id
	 * 
	 * @param dataId
	 *            The Id of the DataResource to check for a Deployment
	 * @return The Deployment for the Resource, if any. Null, if none.
	 */
	public Deployment getDeploymentByDataId(String dataId) throws MongoException {
		BasicDBObject query = new BasicDBObject(DATA_ID, dataId);
		Deployment deployment;

		try {
			deployment = getDeploymentCollection().findOne(query);
		} catch (MongoTimeoutException mte) {
			LOGGER.error(INSTANCE_NOT_AVAILABLE_ERROR, mte);
			throw new MongoException(INSTANCE_NOT_AVAILABLE_ERROR);
		}

		return deployment;
	}

	/**
	 * Gets the Deployment Group by its unique Id.
	 * 
	 * @param deploymentGroupId
	 *            The Id of the Deployment Group
	 * @return The Deployment Group
	 */
	public DeploymentGroup getDeploymentGroupById(String deploymentGroupId) throws MongoException {
		BasicDBObject query = new BasicDBObject(DEPLOYMENTGROUP_ID, deploymentGroupId);
		DeploymentGroup deploymentGroup;

		try {
			deploymentGroup = getDeploymentGroupCollection().findOne(query);
		} catch (MongoTimeoutException mte) {
			LOGGER.error(INSTANCE_NOT_AVAILABLE_ERROR, mte);
			throw new MongoException(INSTANCE_NOT_AVAILABLE_ERROR);
		}

		return deploymentGroup;
	}

	/**
	 * Deletes a deployment entirely from the database.
	 * 
	 * <p>
	 * If a lease exists for this deployment, then it is also removed from the database.
	 * </p>
	 * 
	 * <p>
	 * Note that this is only for database collections only. This does not actually remove the data from GeoServer. This
	 * is handled in the Deployer.
	 * </p>
	 * 
	 * @param deployment
	 *            The deployment to delete
	 */
	public void deleteDeployment(Deployment deployment) throws MongoException {
		// Delete the deployment
		getDeploymentCollection().remove(new BasicDBObject(DEPLOYMENT_ID, deployment.getDeploymentId()));
		// If the deployment had a lease, then delete that too.
		Lease lease = getDeploymentLease(deployment);
		if (lease != null) {
			deleteLease(lease);
		}
	}

	/**
	 * Deletes a Deployment Group.
	 * 
	 * @param deploymentGroup
	 *            The group to delete.
	 */
	public void deleteDeploymentGroup(DeploymentGroup deploymentGroup) throws MongoException {
		getDeploymentGroupCollection().remove(new BasicDBObject(DEPLOYMENTGROUP_ID, deploymentGroup.deploymentGroupId));
	}

	/**
	 * Deletes a lease from the database.
	 * 
	 * <p>
	 * Note that this is only for database collections only. This does not actually remove the data from GeoServer. This
	 * is handled in the Deployer.
	 * </p>
	 * 
	 * @param lease
	 *            The lease to delete.
	 */
	private void deleteLease(Lease lease) throws MongoException {
		getLeaseCollection().remove(new BasicDBObject(LEASE_ID, lease.getLeaseId()));
	}

	/**
	 * Gets the Lease for the Deployment, if on exists.
	 * 
	 * @param deployment
	 *            The Deployment
	 * @return The Lease for the Deployment, if it exists. Null if not.
	 */
	public Lease getDeploymentLease(Deployment deployment) throws MongoException {
		BasicDBObject query = new BasicDBObject(DEPLOYMENT_ID, deployment.getDeploymentId());
		Lease lease;

		try {
			lease = getLeaseCollection().findOne(query);
		} catch (MongoTimeoutException mte) {
			LOGGER.error(INSTANCE_NOT_AVAILABLE_ERROR, mte);
			throw new MongoException(INSTANCE_NOT_AVAILABLE_ERROR);
		}

		return lease;
	}

	/**
	 * Gets the DataResource from the Resources collection by Id. This Id is typically what will be returned to the user
	 * as the result of their Job.
	 * 
	 * These Ids are generated by the Ingest component upon ingest of the data. The Ingest component then updates the
	 * Job Manager with the Data Id, which is then sent back to the user. The user will then specify this Data Id in
	 * order to fetch their data.
	 * 
	 * @param dataId
	 *            The Id of the DataResource
	 * @return DataResource object
	 */
	public DataResource getData(String dataId) throws MongoException {
		BasicDBObject query = new BasicDBObject(DATA_ID, dataId);
		DataResource data;

		try {
			if ((data = getDataResourceCollection().findOne(query)) == null) {
				return null;
			}
		} catch (MongoTimeoutException mte) {
			LOGGER.error(INSTANCE_NOT_AVAILABLE_ERROR, mte);
			throw new MongoException(INSTANCE_NOT_AVAILABLE_ERROR);
		}

		return data;
	}

	/**
	 * Gets a Deployment by its unique Id.
	 * 
	 * @param deploymentId
	 *            The deployment Id
	 * @return The Deployment
	 */
	public Deployment getDeployment(String deploymentId) throws MongoException {
		BasicDBObject query = new BasicDBObject(DEPLOYMENT_ID, deploymentId);
		Deployment deployment;

		try {
			if ((deployment = getDeploymentCollection().findOne(query)) == null) {
				return null;
			}
		} catch (MongoTimeoutException mte) {
			LOGGER.error(INSTANCE_NOT_AVAILABLE_ERROR, mte);
			throw new MongoException(INSTANCE_NOT_AVAILABLE_ERROR);
		}

		return deployment;
	}

	/**
	 * Updates the Expiration date for the Lease.
	 * 
	 * @param leaseId
	 *            The Id of the lease to update.
	 * @param expirationDate
	 *            The new Expiration date. ISO8601 String.
	 */
	public void updateLeaseExpirationDate(String leaseId, String expirationDate) throws MongoException {
		getLeaseCollection().update(DBQuery.is(LEASE_ID, leaseId), DBUpdate.set("expirationDate", expirationDate));
	}

	/**
	 * Updates the status of a Deployment Group to mark if an accompanying Layer Group in GeoServer has been created.
	 * 
	 * @param deploymentGroupId
	 *            The Id of the Deployment Group
	 * @param created
	 *            Whether or not the Deployment Group has an accompanying Layer Group in the GeoServer instance.
	 */
	public void updateDeploymentGroupCreated(String deploymentGroupId, boolean created) throws MongoException {
		getDeploymentGroupCollection().update(DBQuery.is(DEPLOYMENTGROUP_ID, deploymentGroupId),
				DBUpdate.set("hasGisServerLayer", created));
	}

	/**
	 * Creates a new Deployment entry in the database.
	 * 
	 * @param deployment
	 *            Deployment to enter
	 */
	public void insertDeployment(Deployment deployment) throws MongoException {
		getDeploymentCollection().insert(deployment);
	}

	/**
	 * Creates a new Deployment Group entry in the database.
	 * 
	 * @param deploymentGroup
	 *            Deployment Group to insert
	 */
	public void insertDeploymentGroup(DeploymentGroup deploymentGroup) throws MongoException {
		getDeploymentGroupCollection().insert(deploymentGroup);
	}

	/**
	 * Creates a new Lease entry in the database.
	 * 
	 * @param lease
	 *            Lease to enter
	 */
	public void insertLease(Lease lease) throws MongoException {
		getLeaseCollection().insert(lease);
	}

	/**
	 * Gets the Mongo Collection of all data currently referenced within Piazza.
	 * 
	 * @return Mongo collection for DataResources
	 */
	public JacksonDBCollection<DataResource, String> getDataResourceCollection() {
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(resourceCollectionName);
		return JacksonDBCollection.wrap(collection, DataResource.class, String.class);
	}

	/**
	 * Gets a list of data from the database
	 * 
	 * @param page
	 *            The page number to start at
	 * @param pageSize
	 *            The number of results per page
	 * @param sortBy
	 *            The field to sort by
	 * @param order
	 *            The order "asc" or "desc"
	 * @param keyword
	 *            Keyword filtering
	 * @param userName
	 *            Username filtering
	 * @param createdByJobId
	 *            Filter by the ID of the Job that created this Data
	 * @return List of Data items
	 */
	public DataResourceListResponse getDataList(Integer page, Integer pageSize, String sortBy, String order, String keyword,
			String userName, String createdByJobId) throws MongoException {
		
		// Get a DB Cursor to the query for general data
		DBCursor<DataResource> cursor = getDataResourceCollection().find();
		if(keyword != null)
		{
			Pattern regex = Pattern.compile(String.format("(?i)%s", keyword));
			cursor = cursor.or(DBQuery.regex("metadata.name", regex), DBQuery.regex("metadata.description", regex));
		}

		if ((userName != null) && !(userName.isEmpty())) {
			cursor.and(DBQuery.is("metadata.createdBy", userName));
		}
		if ((createdByJobId != null) && !(createdByJobId.isEmpty())) {
			cursor.and(DBQuery.is("metadata.createdByJobId", createdByJobId));
		}

		// Sort and order
		if ("asc".equalsIgnoreCase(order)) {
			cursor = cursor.sort(DBSort.asc(sortBy));
		} else if ("desc".equalsIgnoreCase(order)) {
			cursor = cursor.sort(DBSort.desc(sortBy));
		}

		Integer size = Integer.valueOf(cursor.size());
		// Filter the data by pages
		List<DataResource> data = cursor.skip(page * pageSize).limit(pageSize).toArray();
		// Attach pagination information
		Pagination pagination = new Pagination(size, page, pageSize, sortBy, order);
		// Create the Response and send back
		return new DataResourceListResponse(data, pagination);
	}

	/**
	 * Returns the number of items in the database for Data Resources
	 * 
	 * @return number of Data Resources in the database
	 */
	public long getDataCount() {
		return getDataResourceCollection().count();
	}

	/**
	 * Gets a list of deployments from the database
	 * 
	 * @param page
	 *            The page number to start
	 * @param pageSize
	 *            The number of results per page
	 * @param sortBy
	 *            The field to sort by
	 * @param order
	 *            The order "asc" or "desc"
	 * @param keyword
	 *            Keyword filtering
	 * @return List of deployments
	 */
	public DeploymentListResponse getDeploymentList(Integer page, Integer pageSize, String sortBy, String order, String keyword)
			throws MongoException {
		Pattern regex = Pattern.compile(String.format("(?i)%s", keyword != null ? keyword : ""));
		// Get a DB Cursor to the query for general data
		DBCursor<Deployment> cursor = getDeploymentCollection().find().or(DBQuery.regex(DEPLOYMENT_ID, regex),
				DBQuery.regex(DATA_ID, regex), DBQuery.regex("capabilitiesUrl", regex));

		// Sort and order
		if ("asc".equalsIgnoreCase(order)) {
			cursor = cursor.sort(DBSort.asc(sortBy));
		} else if ("desc".equalsIgnoreCase(order)) {
			cursor = cursor.sort(DBSort.desc(sortBy));
		}

		Integer size = Integer.valueOf(cursor.size());
		// Filter the data by pages
		List<Deployment> data = cursor.skip(page * pageSize).limit(pageSize).toArray();
		// Attach pagination information
		Pagination pagination = new Pagination(size, page, pageSize, sortBy, order);
		// Create the Response and send back
		return new DeploymentListResponse(data, pagination);
	}

	/**
	 * Gets the Mongo Collection of all Deployments currently referenced within Piazza.
	 * 
	 * @return Mongo collection for Deployments
	 */
	public JacksonDBCollection<Deployment, String> getDeploymentCollection() {
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(deploymentCollectionName);
		return JacksonDBCollection.wrap(collection, Deployment.class, String.class);
	}

	/**
	 * Gets the Mongo Collection of all Deployment Groups currently referenced within Piazza.
	 * 
	 * @return Mongo collection for Deployment Groups
	 */
	public JacksonDBCollection<DeploymentGroup, String> getDeploymentGroupCollection() {
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(deploymentGroupCollectionName);
		return JacksonDBCollection.wrap(collection, DeploymentGroup.class, String.class);
	}

	/**
	 * Gets the Mongo Collection of all Leases currently referenced within Piazza.
	 * 
	 * @return Mongo collection for Leases
	 */
	public JacksonDBCollection<Lease, String> getLeaseCollection() {
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(leaseCollectionName);
		return JacksonDBCollection.wrap(collection, Lease.class, String.class);
	}
}
