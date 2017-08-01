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
import org.geotools.data.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.venice.piazza.common.hibernate.dao.DataResourceDao;
import org.venice.piazza.common.hibernate.dao.DeploymentDao;
import org.venice.piazza.common.hibernate.dao.DeploymentGroupDao;
import org.venice.piazza.common.hibernate.dao.LeaseDao;
import org.venice.piazza.common.hibernate.entity.DataResourceEntity;
import org.venice.piazza.common.hibernate.entity.DeploymentEntity;
import org.venice.piazza.common.hibernate.entity.DeploymentGroupEntity;
import org.venice.piazza.common.hibernate.entity.LeaseEntity;

import com.google.common.collect.Lists;

import model.data.DataResource;
import model.data.deployment.Deployment;
import model.data.deployment.DeploymentGroup;
import model.data.deployment.Lease;
import model.response.DataResourceListResponse;
import model.response.DeploymentListResponse;

/**
 * Handles database access for the Deployer and the Leaser, and Resource collections which stores the Ingested Resource metadata.
 * 
 * @author Sonny.Saniev
 * 
 */
@Component
public class DatabaseAccessor {
	@Value("${mongo.db.collection.resources}")
	private String resourceCollectionName;
	@Value("${mongo.db.collection.deployments}")
	private String deploymentCollectionName;
	@Value("${mongo.db.collection.deployment.groups}")
	private String deploymentGroupCollectionName;
	@Value("${mongo.db.collection.leases}")
	private String leaseCollectionName;


	@Autowired
	private Environment environment;

	@Autowired
	private DataResourceDao dataResourceDao;

	@Autowired
	private LeaseDao leaseDao;
	
	@Autowired
	private DeploymentDao deploymentDao;

	@Autowired
	private DeploymentGroupDao deploymentGroupDao;
	
	private static final String DATA_ID = "dataId";
	private static final String DEPLOYMENT_ID = "deploymentId";
	private static final String DEPLOYMENTGROUP_ID = "deploymentGroupId";
	private static final String LEASE_ID = "leaseId";
	private static final String INSTANCE_NOT_AVAILABLE_ERROR = "MongoDB instance not available.";
	private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseAccessor.class);

//	/**
//	 * Gets the PostGIS data store for GeoTools.
//	 * 
//	 * @return Data Store.
//	 */
	public DataStore getPostGisDataStore(String host, String port, String schema, String dbName, String user, String password)
			throws IOException {
//		return GeoToolsUtil.getPostGisDataStore(host, port, schema, dbName, user, password);
//		
		return null;
	}

//	/**
//	 * Gets the Deployment for the specified Resource Id
//	 * 
//	 * @param dataId
//	 *            The Id of the DataResource to check for a Deployment
//	 * @return The Deployment for the Resource, if any. Null, if none.
//	 */
	public Deployment getDeploymentByDataId(String dataId) {
//		BasicDBObject query = new BasicDBObject(DATA_ID, dataId);
//		Deployment deployment;
//
//		try {
//			deployment = getDeploymentCollection().findOne(query);
//		} catch (MongoTimeoutException mte) {
//			LOGGER.error(INSTANCE_NOT_AVAILABLE_ERROR, mte);
//			throw new MongoException(INSTANCE_NOT_AVAILABLE_ERROR);
//		}
//
//		return deployment;
//		
		return null;
	}

//	/**
//	 * Gets the Deployment Group by its unique Id.
//	 * 
//	 * @param deploymentGroupId
//	 *            The Id of the Deployment Group
//	 * @return The Deployment Group
//	 */
	public DeploymentGroup getDeploymentGroupById(String deploymentGroupId) {
//		BasicDBObject query = new BasicDBObject(DEPLOYMENTGROUP_ID, deploymentGroupId);
//		DeploymentGroup deploymentGroup;
//
//		try {
//			deploymentGroup = getDeploymentGroupCollection().findOne(query);
//		} catch (MongoTimeoutException mte) {
//			LOGGER.error(INSTANCE_NOT_AVAILABLE_ERROR, mte);
//			throw new MongoException(INSTANCE_NOT_AVAILABLE_ERROR);
//		}
//
//		return deploymentGroup;
//		
		return null;
	}

//	/**
//	 * Deletes a deployment entirely from the database.
//	 * 
//	 * If a lease exists for this deployment, then it is also removed from the database.
//	 * 
//	 * Note that this is only for database collections only. This does not actually remove the data from GeoServer. This
//	 * is handled in the Deployer.
//	 * 
//	 * @param deployment
//	 *            The deployment to delete
//	 */
	public void deleteDeployment(Deployment deployment) {
//		// Delete the deployment
//		getDeploymentCollection().remove(new BasicDBObject(DEPLOYMENT_ID, deployment.getDeploymentId()));
//		// If the deployment had a lease, then delete that too.
//		Lease lease = getDeploymentLease(deployment);
//		if (lease != null) {
//			deleteLease(lease);
//		}
	}

//	/**
//	 * Deletes a Deployment Group.
//	 * 
//	 * @param deploymentGroup
//	 *            The group to delete.
//	 */
	public void deleteDeploymentGroup(DeploymentGroup deploymentGroup) {
		//getDeploymentGroupCollection().remove(new BasicDBObject(DEPLOYMENTGROUP_ID, deploymentGroup.deploymentGroupId));
	}

	/**
	 * Deletes a lease from the database.
	 * 
	 * Note that this is only for database collections only. This does not actually remove the data from GeoServer. That
	 * is handled in the Deployer.
	 * 
	 * @param lease
	 *            The lease to delete.
	 */
	private void deleteLease(Lease lease) {
		LeaseEntity record = leaseDao.findOneLeaseById(lease.getLeaseId());
		if( record != null)
			leaseDao.delete(record);
	}

	/**
	 * Gets the Lease for the Deployment, if on exists.
	 * 
	 * @param deployment
	 *            The Deployment
	 * @return The Lease for the Deployment, if it exists. Null if not.
	 */
	public Lease getDeploymentLease(Deployment deployment) {
		Lease lease = null;
		LeaseEntity record = leaseDao.findOneLeaseByDeploymentId(deployment.getDeploymentId());
		if( record != null )
			lease = record.getLease();
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
	public DataResource getData(String dataId) {
		DataResource dataResource = null;
		DataResourceEntity record = dataResourceDao.fineOneRecord(dataId);
		if (record != null) {
			dataResource = record.getDataResource();
		}

		return dataResource;
	}

//	/**
//	 * Gets a Deployment by its unique Id.
//	 * 
//	 * @param deploymentId
//	 *            The deployment Id
//	 * @return The Deployment
//	 */
	public Deployment getDeployment(String deploymentId) {
//		BasicDBObject query = new BasicDBObject(DEPLOYMENT_ID, deploymentId);
//		Deployment deployment;
//
//		try {
//			if ((deployment = getDeploymentCollection().findOne(query)) == null) {
//				return null;
//			}
//		} catch (MongoTimeoutException mte) {
//			LOGGER.error(INSTANCE_NOT_AVAILABLE_ERROR, mte);
//			throw new MongoException(INSTANCE_NOT_AVAILABLE_ERROR);
//		}

		return null;
	}

	/**
	 * Updates the Expiration date for the Lease.
	 * 
	 * @param leaseId
	 *            The Id of the lease to update.
	 * @param expirationDate
	 *            The new Expiration date. ISO8601 String.
	 */
	public void updateLeaseExpirationDate(String leaseId, String expirationDate) {
		LeaseEntity record = leaseDao.findOneLeaseById(leaseId);
		if (record != null) {
			record.getLease().setExpiresOn(expirationDate);
		}
		leaseDao.save(record);
	}

//
//	/**
//	 * Updates the status of a Deployment Group to mark if an accompanying Layer Group in GeoServer has been created.
//	 * 
//	 * @param deploymentGroupId
//	 *            The Id of the Deployment Group
//	 * @param created
//	 *            Whether or not the Deployment Group has an accompanying Layer Group in the GeoServer instance.
//	 */
	public void updateDeploymentGroupCreated(String deploymentGroupId, boolean created) {
//		getDeploymentGroupCollection().update(DBQuery.is(DEPLOYMENTGROUP_ID, deploymentGroupId),
//				DBUpdate.set("hasGisServerLayer", created));
	}

//	/**
//	 * Creates a new Deployment entry in the database.
//	 * 
//	 * @param deployment
//	 *            Deployment to enter
//	 */
	public void insertDeployment(Deployment deployment) {
		//getDeploymentCollection().insert(deployment);
	}
//
//	/**
//	 * Creates a new Deployment Group entry in the database.
//	 * 
//	 * @param deploymentGroup
//	 *            Deployment Group to insert
//	 */
	public void insertDeploymentGroup(DeploymentGroup deploymentGroup) {
		//getDeploymentGroupCollection().insert(deploymentGroup);
	}

	/**
	 * Creates a new Lease entry in the database.
	 * 
	 * @param lease
	 *            Lease to enter
	 */
	public void insertLease(Lease lease) {
		LeaseEntity newRecord = new LeaseEntity();
		newRecord.setLease(lease);
		leaseDao.save(newRecord);
	}

	/**
	 * Gets the Collection of all data currently referenced within Piazza.
	 * 
	 * @return Iterable type of DataResourcesEntity
	 */
	public Iterable<DataResourceEntity> getDataResourceCollection() {
		return dataResourceDao.findAll();
	}

//	/**
//	 * Gets a list of data from the database
//	 * 
//	 * @param page
//	 *            The page number to start at
//	 * @param pageSize
//	 *            The number of results per page
//	 * @param sortBy
//	 *            The field to sort by
//	 * @param order
//	 *            The order "asc" or "desc"
//	 * @param keyword
//	 *            Keyword filtering
//	 * @param userName
//	 *            Username filtering
//	 * @param createdByJobId
//	 *            Filter by the ID of the Job that created this Data
//	 * @return List of Data items
//	 */
	public DataResourceListResponse getDataList(Integer page, Integer pageSize, String sortBy, String order, String keyword,
			String userName, String createdByJobId){
//		
//		// Get a DB Cursor to the query for general data
//		DBCursor<DataResource> cursor = getDataResourceCollection().find();
//		if (keyword != null && !keyword.isEmpty()) {
//			Pattern regex = Pattern.compile(String.format("(?i)%s", keyword));
//			cursor = cursor.or(DBQuery.regex("metadata.name", regex), DBQuery.regex("metadata.description", regex));
//		}
//
//		if ((userName != null) && !(userName.isEmpty())) {
//			cursor.and(DBQuery.is("metadata.createdBy", userName));
//		}
//		if ((createdByJobId != null) && !(createdByJobId.isEmpty())) {
//			cursor.and(DBQuery.is("metadata.createdByJobId", createdByJobId));
//		}
//
//		// Sort and order
//		if ("asc".equalsIgnoreCase(order)) {
//			cursor = cursor.sort(DBSort.asc(sortBy));
//		} else if ("desc".equalsIgnoreCase(order)) {
//			cursor = cursor.sort(DBSort.desc(sortBy));
//		}
//
//		Integer size = Integer.valueOf(cursor.size());
//		// Filter the data by pages
//		List<DataResource> data = cursor.skip(page * pageSize).limit(pageSize).toArray();
//		// Attach pagination information
//		Pagination pagination = new Pagination(size, page, pageSize, sortBy, order);
//		// Create the Response and send back
//        return new DataResourceListResponse(data, pagination);
		
		return null;
	}

	/**
	 * Returns the number of items in the database for Data Resources
	 * 
	 * @return number of Data Resources in the database
	 */
	public long getDataCount() {
		return Lists.newArrayList(getDataResourceCollection()).size();
	}

//	/**
//	 * Gets a list of deployments from the database
//	 * 
//	 * @param page
//	 *            The page number to start
//	 * @param pageSize
//	 *            The number of results per page
//	 * @param sortBy
//	 *            The field to sort by
//	 * @param order
//	 *            The order "asc" or "desc"
//	 * @param keyword
//	 *            Keyword filtering
//	 * @return List of deployments
//	 */
	public DeploymentListResponse getDeploymentList(Integer page, Integer pageSize, String sortBy, String order, String keyword) {
//		Pattern regex = Pattern.compile(String.format("(?i)%s", keyword != null ? keyword : ""));
//		// Get a DB Cursor to the query for general data
//		DBCursor<Deployment> cursor = getDeploymentCollection().find().or(DBQuery.regex(DEPLOYMENT_ID, regex),
//				DBQuery.regex(DATA_ID, regex), DBQuery.regex("capabilitiesUrl", regex));
//
//		// Sort and order
//		if ("asc".equalsIgnoreCase(order)) {
//			cursor = cursor.sort(DBSort.asc(sortBy));
//		} else if ("desc".equalsIgnoreCase(order)) {
//			cursor = cursor.sort(DBSort.desc(sortBy));
//		}
//
//		Integer size = Integer.valueOf(cursor.size());
//		// Filter the data by pages
//		List<Deployment> data = cursor.skip(page * pageSize).limit(pageSize).toArray();
//		// Attach pagination information
//		Pagination pagination = new Pagination(size, page, pageSize, sortBy, order);
//		// Create the Response and send back
//		return new DeploymentListResponse(data, pagination);
//		
		return null;
	}

	/**
	 * Gets the Iterable collection of all Deployments currently referenced within Piazza.
	 * 
	 * @return Iterable collection for Deployments
	 */
	public Iterable<DeploymentEntity> getDeploymentCollection() {
		return deploymentDao.findAll();
	}

	/**
	 * Gets the Iterable Collection of all Deployment Groups currently referenced within Piazza.
	 * 
	 * @return Iterable collection for Deployment Groups
	 */
	public Iterable<DeploymentGroupEntity> getDeploymentGroupCollection() {
		return deploymentGroupDao.findAll();
	}

	/**
	 * Gets the Iterable collection of all Leases currently referenced within Piazza.
	 * 
	 * @return Iterable collection for Leases
	 */
	public Iterable<LeaseEntity> getLeaseCollection() {
		return leaseDao.findAll();
	}
}
