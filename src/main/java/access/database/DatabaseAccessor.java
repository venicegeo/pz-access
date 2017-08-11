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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.geotools.data.DataStore;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Component;
import org.venice.piazza.common.hibernate.dao.DeploymentGroupDao;
import org.venice.piazza.common.hibernate.dao.LeaseDao;
import org.venice.piazza.common.hibernate.dao.dataresource.DataResourceDao;
import org.venice.piazza.common.hibernate.dao.deployment.DeploymentDao;
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
import model.response.Pagination;
import util.GeoToolsUtil;

/**
 * Handles database access for the Deployer and the Leaser, and Resource collections which stores the Ingested Resource metadata.
 * 
 * @author Sonny.Saniev
 * 
 */
@Component
public class DatabaseAccessor {

	@Autowired
	private DataResourceDao dataResourceDao;

	@Autowired
	private LeaseDao leaseDao;
	
	@Autowired
	private DeploymentDao deploymentDao;

	@Autowired
	private DeploymentGroupDao deploymentGroupDao;
	
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
	public Deployment getDeploymentByDataId(String dataId) {
		Deployment deployment = null;
		DeploymentEntity record = deploymentDao.getDeploymentByDataId(dataId);
		if (record != null) {
			deployment = record.getDeployment();
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
	public DeploymentGroup getDeploymentGroupById(String deploymentGroupId) {
		DeploymentGroup deploymentGroup = null;
		DeploymentGroupEntity record = deploymentGroupDao.findOneDeploymentGroupById(deploymentGroupId);
		if( record != null )
			deploymentGroup = record.getDeploymentGroup();
		return deploymentGroup;
	}

	/**
	 * Deletes a deployment entirely from the database.
	 * 
	 * If a lease exists for this deployment, then it is also removed from the database.
	 * 
	 * Note that this is only for database collections only. This does not actually remove the data from GeoServer. This
	 * is handled in the Deployer.
	 * 
	 * @param deployment
	 *            The deployment to delete
	 */
	public void deleteDeployment(Deployment deployment) {
		if (deployment != null) {
			DeploymentEntity record = deploymentDao.getDeploymentByDeploymentId(deployment.getDeploymentId());
			if (record != null) {
				deploymentDao.delete(record);
			}
		}
	}

	/**
	 * Deletes a Deployment Group.
	 * 
	 * @param deploymentGroup
	 *            The group to delete.
	 */
	public void deleteDeploymentGroup(DeploymentGroup deploymentGroup) {
		if (deploymentGroup != null) {
			DeploymentGroupEntity record = deploymentGroupDao.findOneDeploymentGroupById(deploymentGroup.deploymentGroupId);
			if (record != null)
				deploymentGroupDao.delete(record);
		}
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
		DataResourceEntity record = dataResourceDao.getDataResourceByDataId(dataId);
		if (record != null) {
			dataResource = record.getDataResource();
		}

		return dataResource;
	}

	/**
	 * Gets a Deployment by its unique Id.
	 * 
	 * @param deploymentId
	 *            The deployment Id
	 * @return The Deployment
	 */
	public Deployment getDeployment(String deploymentId) {
		Deployment deployment = null;
		DeploymentEntity record = deploymentDao.getDeploymentByDeploymentId(deploymentId);
		if (record != null) {
			deployment = record.getDeployment();
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
	public void updateLeaseExpirationDate(String leaseId, String expirationDate) {
		LeaseEntity record = leaseDao.findOneLeaseById(leaseId);
		if (record != null) {
			record.getLease().setExpiresOn(DateTime.parse(expirationDate));
		}
		leaseDao.save(record);
	}


	/**
	 * Updates the status of a Deployment Group to mark if an accompanying Layer Group in GeoServer has been created.
	 * 
	 * @param deploymentGroupId
	 *            The Id of the Deployment Group
	 * @param created
	 *            Whether or not the Deployment Group has an accompanying Layer Group in the GeoServer instance.
	 */
	public void updateDeploymentGroupCreated(String deploymentGroupId, boolean created) {
		DeploymentGroupEntity record = deploymentGroupDao.findOneDeploymentGroupById(deploymentGroupId);
		if( record !=null)
		{
			record.getDeploymentGroup().setHasGisServerLayer(created);
		}
		
		deploymentGroupDao.save(record);
	}

	/**
	 * Creates a new Deployment entry in the database.
	 * 
	 * @param deployment
	 *            Deployment to enter
	 */
	public void insertDeployment(Deployment deployment) {
		DeploymentEntity newRecord = new DeploymentEntity();
		newRecord.setDeployment(deployment);
		deploymentDao.save(newRecord);
	}

	/**
	 * Creates a new Deployment Group entry in the database.
	 * 
	 * @param deploymentGroup
	 *            Deployment Group to insert
	 */
	public void insertDeploymentGroup(DeploymentGroup deploymentGroup) {
		DeploymentGroupEntity newRecord = new DeploymentGroupEntity();
		newRecord.setDeploymentGroup(deploymentGroup);
		deploymentGroupDao.save(newRecord);
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

	/**
	 * Returns the number of items in the database for Data Resources
	 * 
	 * @return number of Data Resources in the database
	 */
	public long getDataCount() {
		return Lists.newArrayList(getDataResourceCollection()).size();
	}



	/**
	 * 
	 * @param date
	 *			
	 * @return
	 */
	public Iterable<Lease> getExpiredLeases(DateTime date)
	{
		List<Lease> list = new ArrayList<Lease>();
		for( LeaseEntity record : leaseDao.findExpiredLeases(date.getMillis()))
		{
			list.add(record.getLease());
		}
		return list;
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
	public DeploymentListResponse getDeploymentList(Integer page, Integer pageSize, String sortBy, String order, String keyword) {
		Pagination pagination = new Pagination(null, page, pageSize, sortBy, order);
		Page<DeploymentEntity> results = null;

		if (StringUtils.isNotEmpty(keyword)) {
			results = deploymentDao.getDeploymentListByDeploymentId(keyword, pagination);
			if (results == null) {
				results = deploymentDao.getDeploymentListByDataId(keyword, pagination);
			}
			if (results == null) {
				results = deploymentDao.getDeploymentListByCapabilitiesUrl(keyword, pagination);
			}
		} else {
			results = deploymentDao.getDeploymentList(pagination);
		}

		// Collect the Deployments
		List<Deployment> deployments = new ArrayList<Deployment>();
		for (DeploymentEntity entity : results) {
			deployments.add(entity.getDeployment());
		}
		// Set Pagination count
		pagination.setCount(results.getTotalElements());

		// Return the complete List
		return new DeploymentListResponse(deployments, pagination);
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
			String userName, String createdByJobId) {

		Pagination pagination = new Pagination(null, page, pageSize, sortBy, order);
		Page<DataResourceEntity> results = null;

		if (StringUtils.isNotEmpty(userName) && StringUtils.isNotEmpty(keyword)) {
			// Both parameters specified
			results = dataResourceDao.getDataResourceForUserAndKeyword(keyword, userName, pagination);
		} else if (StringUtils.isNotEmpty(userName)) {
			// Query by User
			results = dataResourceDao.getDataResourceListByUser(userName, pagination);
		} else if (StringUtils.isNotEmpty(keyword)) {
			// Query by Keyword
			results = dataResourceDao.getDataResourceListByKeyword(keyword, pagination);
		} else if (StringUtils.isNotEmpty(createdByJobId)) {
			// Query by Keyword
			results = dataResourceDao.getDataResourceListByCreatedJobId(createdByJobId, pagination);
		} else {
			// Query all Jobs
			results = dataResourceDao.getDataResourceList(pagination);
		}

		// Collect the Jobs
		List<DataResource> dataResources = new ArrayList<DataResource>();
		for (DataResourceEntity dataResourceEntity : results) {
			dataResources.add(dataResourceEntity.getDataResource());
		}
		// Set Pagination count
		pagination.setCount(results.getTotalElements());

		// Return the complete List
		return new DataResourceListResponse(dataResources, pagination);
	}
}
