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

import java.net.UnknownHostException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import model.data.DataResource;
import model.data.deployment.Deployment;
import model.data.deployment.Lease;

import org.mongojack.DBQuery;
import org.mongojack.DBUpdate;
import org.mongojack.JacksonDBCollection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;

/**
 * Handles Mongo access for the Deployer and the Leaser, and for the Resource
 * collection which stores the Ingested Resource metadata.
 * 
 * Deployments and leases have their own collections, and are managed by this
 * Access component.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class MongoAccessor {
	@Value("${mongo.host}")
	private String DATABASE_HOST;
	@Value("${mongo.port}")
	private int DATABASE_PORT;
	@Value("${mongo.db.name}")
	private String DATABASE_NAME;
	@Value("${mongo.db.collection.resources}")
	private String RESOURCE_COLLECTION_NAME;
	@Value("${mongo.db.collection.deployments}")
	private String DEPLOYMENT_COLLECTION_NAME;
	@Value("${mongo.db.collection.leases}")
	private String LEASE_COLLECTION_NAME;
	private MongoClient mongoClient;

	@PostConstruct
	private void initialize() {
		try {
			mongoClient = new MongoClient(DATABASE_HOST, DATABASE_PORT);
		} catch (UnknownHostException exception) {
			System.out.println("Error connecting to MongoDB Instance.");
			exception.printStackTrace();
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
	 * Gets the Deployment for the specified Resource ID
	 * 
	 * @param dataId
	 *            The ID of the DataResource to check for a Deployment
	 * @return The Deployment for the Resource, if any. Null, if none.
	 */
	public Deployment getDeploymentByDataId(String dataId) {
		BasicDBObject query = new BasicDBObject("dataId", dataId);
		Deployment deployment;

		try {
			deployment = getDeploymentCollection().findOne(query);
		} catch (MongoTimeoutException mte) {
			throw new MongoException("MongoDB instance not available.");
		}

		return deployment;
	}

	/**
	 * Gets the Lease for the Deployment, if on exists.
	 * 
	 * @param deployment
	 *            The Deployment
	 * @return The Lease for the Deployment, if it exists. Null if not.
	 */
	public Lease getDeploymentLease(Deployment deployment) {
		BasicDBObject query = new BasicDBObject("deploymentId", deployment.getId());
		Lease lease;

		try {
			lease = getLeaseCollection().findOne(query);
		} catch (MongoTimeoutException mte) {
			throw new MongoException("MongoDB instance not available.");
		}

		return lease;
	}

	/**
	 * Gets the DataResource from the Resources collection by ID. This ID is
	 * typically what will be returned to the user as the result of their Job.
	 * 
	 * These IDs are generated by the Ingest component upon ingest of the data.
	 * The Ingest component then updates the Job Manager with the Data ID, which
	 * is then sent back to the user. The user will then specify this Data ID in
	 * order to fetch their data.
	 * 
	 * @param dataId
	 *            The ID of the DataResource
	 * @return DataResource object
	 */
	public DataResource getData(String dataId) {
		BasicDBObject query = new BasicDBObject("dataId", dataId);
		DataResource data;

		try {
			if ((data = getDataResourceCollection().findOne(query)) == null) {
				throw new ResourceAccessException("Data not found.");
			}
		} catch (MongoTimeoutException mte) {
			throw new MongoException("MongoDB instance not available.");
		}

		return data;
	}

	/**
	 * Updates the Expiration date for the Lease.
	 * 
	 * @param leaseId
	 *            The ID of the lease to update.
	 * @param expirationDate
	 *            The new Expiration date. ISO8601 String.
	 */
	public void updateLeaseExpirationDate(String leaseId, String expirationDate) {
		getLeaseCollection().update(DBQuery.is("id", leaseId), DBUpdate.set("expirationDate", expirationDate));
	}

	/**
	 * Creates a new Deployment entry in the database.
	 * 
	 * @param deployment
	 *            Deployment to enter
	 */
	public void insertDeployment(Deployment deployment) {
		getDeploymentCollection().insert(deployment);
	}

	/**
	 * Creates a new Lease entry in the database.
	 * 
	 * @param lease
	 *            Lease to enter
	 */
	public void insertLease(Lease lease) {
		getLeaseCollection().insert(lease);
	}

	/**
	 * Gets the Mongo Collection of all data currently referenced within Piazza.
	 * 
	 * @return Mongo collection for DataResources
	 */
	private JacksonDBCollection<DataResource, String> getDataResourceCollection() {
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(RESOURCE_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, DataResource.class, String.class);
	}

	/**
	 * Gets the Mongo Collection of all Deployments currently referenced within
	 * Piazza.
	 * 
	 * @return Mongo collection for Deployments
	 */
	private JacksonDBCollection<Deployment, String> getDeploymentCollection() {
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(DEPLOYMENT_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, Deployment.class, String.class);
	}

	/**
	 * Gets the Mongo Collection of all Leases currently referenced within
	 * Piazza.
	 * 
	 * @return Mongo collection for Leases
	 */
	private JacksonDBCollection<Lease, String> getLeaseCollection() {
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(LEASE_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, Lease.class, String.class);
	}
}
