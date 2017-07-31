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
package access.controller;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.geotools.data.DataStore;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geojson.feature.FeatureJSON;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpStatusCodeException;

import com.amazonaws.util.StringUtils;

import access.database.DatabaseAccessor;
import access.deploy.Deployer;
import access.deploy.GroupDeployer;
import access.deploy.Leaser;
import access.messaging.AccessThreadManager;
import access.util.AccessUtilities;
import exception.InvalidInputException;
import model.data.DataResource;
import model.data.FileRepresentation;
import model.data.deployment.Deployment;
import model.data.deployment.DeploymentGroup;
import model.data.deployment.Lease;
import model.data.type.PostGISDataType;
import model.data.type.TextDataType;
import model.logger.AuditElement;
import model.logger.Severity;
import model.response.DataResourceResponse;
import model.response.DeploymentGroupResponse;
import model.response.DeploymentResponse;
import model.response.ErrorResponse;
import model.response.PiazzaResponse;
import model.response.SuccessResponse;
import util.PiazzaLogger;

/**
 * Allows for synchronous fetching of Resource Data from the Mongo Resource collection.
 * 
 * The collection is bound to the DataResource model.
 * 
 * This controller is similar to the functionality of the JobManager REST Controller, in that this component primarily
 * listens for messages via Kafka, however, for instances where the user needs a direct read out of the database - this
 * should be a synchronous response that does not involve Kafka. For such requests, this REST controller exists.
 * 
 * @author Patrick.Doody
 * 
 */
@RestController
public class AccessController {

	@Value("${vcap.services.pz-geoserver-efs.credentials.postgres.hostname}")
	private String postgresHost;
	@Value("${vcap.services.pz-geoserver-efs.credentials.postgres.port}")
	private String postgresPort;
	@Value("${vcap.services.pz-geoserver-efs.credentials.postgres.database}")
	private String postgresDBName;
	@Value("${vcap.services.pz-geoserver-efs.credentials.postgres.username}")
	private String postgresUser;
	@Value("${vcap.services.pz-geoserver-efs.credentials.postgres.password}")
	private String postgresPassword;
	@Value("${postgres.schema}")
	private String postgresSchema;

	@Autowired
	private AccessThreadManager threadManager;
	@Autowired
	private PiazzaLogger pzLogger;
	@Autowired
	private DatabaseAccessor accessor;
	@Autowired
	private Deployer deployer;
	@Autowired
	private GroupDeployer groupDeployer;
	@Autowired
	private Leaser leaser;
	@Autowired
	private ThreadPoolTaskExecutor threadPoolTaskExecutor;
	@Autowired
	private AccessUtilities accessUtilities;

	private static final String DEFAULT_PAGE_SIZE = "10";
	private static final String DEFAULT_PAGE = "0";
	private static final String DEFAULT_SORTBY = "dataId";
	private static final String DEFAULT_ORDER = "asc";

	private static final Logger LOGGER = LoggerFactory.getLogger(AccessController.class);

	private static final String ACCESS_COMPONENT_NAME = "Access";
	private static final String ACCESS = "access";

	/**
	 * Healthcheck required for all Piazza Core Services
	 * 
	 * @return String
	 */
	@RequestMapping(value = "/", method = RequestMethod.GET)
	public String getHealthCheck() {
		return "Hello, Health Check here for pz-access.";
	}

	/**
	 * Requests a file download that has been prepared by this Access component. This will return the raw bytes of the
	 * resource.
	 * 
	 * @param dataId
	 *            The Id of the Data Item to get. Assumes this file is ready to be downloaded.
	 */
	@SuppressWarnings("rawtypes")
	@RequestMapping(value = "/file/{dataId}", method = RequestMethod.GET)
	public ResponseEntity accessFile(@PathVariable(value = "dataId") String dataId,
			@RequestParam(value = "fileName", required = false) String name) {
		
		final String returnAction = "returningFileBytes";
		
		try {
			// Get the DataResource item
			DataResource data = accessor.getData(dataId);
			String fileName = StringUtils.isNullOrEmpty(name) ? dataId : name;
			pzLogger.log(String.format("Processing Data File for %s", dataId), Severity.INFORMATIONAL,
					new AuditElement(ACCESS, "beginProcessingFile", dataId));

			if (data == null) {
				pzLogger.log(String.format("Data not found for requested Id %s", dataId), Severity.WARNING);
				return new ResponseEntity<>(new ErrorResponse(String.format("Data not found: %s", dataId), ACCESS_COMPONENT_NAME),
						HttpStatus.NOT_FOUND);
			}

			if (data.getDataType() instanceof TextDataType) {
				// Stream the Bytes back
				TextDataType textData = (TextDataType) data.getDataType();
				pzLogger.log(String.format("Returning Bytes for %s", dataId), Severity.INFORMATIONAL,
						new AuditElement(ACCESS, returnAction, dataId));
				return getResponse(MediaType.TEXT_PLAIN, String.format("%s%s", fileName, ".txt"), textData.getContent().getBytes());
			} else if (data.getDataType() instanceof PostGISDataType) {
				// Obtain geoJSON from postGIS
				StringBuilder geoJSON = getPostGISGeoJSON(data);

				// Log the Request
				pzLogger.log(String.format("Returning Bytes for %s of length %s", dataId, geoJSON.length()), Severity.INFORMATIONAL,
						new AuditElement(ACCESS, returnAction, dataId));

				// Stream the Bytes back
				return getResponse(MediaType.TEXT_PLAIN, String.format("%s%s", fileName, ".geojson"), geoJSON.toString().getBytes());
			} else if (!(data.getDataType() instanceof FileRepresentation)) {
				String message = String.format("File download not available for Data Id %s; type is %s", dataId,
						data.getDataType().getClass().getSimpleName());
				pzLogger.log(message, Severity.WARNING, new AuditElement(ACCESS, "accessBytesError", ""));
				throw new InvalidInputException(message);
			} else {
				byte[] bytes = accessUtilities.getBytesForDataResource(data);

				// Log the Request
				pzLogger.log(String.format("Returning Bytes for %s of length %s", dataId, bytes.length), Severity.INFORMATIONAL,
						new AuditElement(ACCESS, returnAction, dataId));

				// Preserve the file extension from the original file.
				String originalFileName = ((FileRepresentation) data.getDataType()).getLocation().getFileName();
				String extension = FilenameUtils.getExtension(originalFileName);

				// Stream the Bytes back
				return getResponse(MediaType.APPLICATION_OCTET_STREAM, String.format("%s.%s", fileName, extension), bytes);
			}
		} catch (Exception exception) {
			String error = String.format("Error fetching Data %s: %s", dataId, exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR, new AuditElement(ACCESS, "errorAccessingBytes", dataId));
			return new ResponseEntity<>(new ErrorResponse("Error fetching File: " + exception.getMessage(), ACCESS_COMPONENT_NAME),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Returns the Data resource object from the Resources collection.
	 * 
	 * @param dataId
	 *            Id of the Resource
	 * @return The resource matching the specified Id
	 */
	@RequestMapping(value = "/data/{dataId}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> getData(@PathVariable(value = "dataId") String dataId) {
		try {
			if (dataId.isEmpty()) {
				throw new InvalidInputException("No Data Id specified.");
			}
			// Query for the Data Id
			DataResource data = accessor.getData(dataId);
			if (data == null) {
				pzLogger.log(String.format("Data not found for requested Id %s", dataId), Severity.WARNING);
				return new ResponseEntity<>(new ErrorResponse(String.format("Data not found: %s", dataId), ACCESS_COMPONENT_NAME),
						HttpStatus.NOT_FOUND);
			}

			// Return the Data Resource item
			pzLogger.log(String.format("Returning Data Metadata for %s", dataId), Severity.INFORMATIONAL);
			return new ResponseEntity<>(new DataResourceResponse(data), HttpStatus.OK);
		} catch (Exception exception) {
			String error = String.format("Error fetching Data %s: %s", dataId, exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR, new AuditElement(ACCESS, "errorGettingMetadata", dataId));
			return new ResponseEntity<>(new ErrorResponse("Error fetching Data: " + exception.getMessage(), ACCESS_COMPONENT_NAME),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Gets Deployment information for an active deployment, including URL and Data Id.
	 * 
	 * @see http://pz-swagger/#!/Deployment/ get_deployment_deploymentId
	 * 
	 * @param deploymentId
	 *            The Id of the deployment to fetch
	 * @return The deployment information, or an ErrorResponse if exceptions occur
	 */
	@RequestMapping(value = "/deployment/{deploymentId}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> getDeployment(@PathVariable(value = "deploymentId") String deploymentId) {
		try {
			if (deploymentId.isEmpty()) {
				throw new InvalidInputException("No Deployment Id specified.");
			}
			// Query for the Deployment Id
			Deployment deployment = accessor.getDeployment(deploymentId);
			if (deployment == null) {
				pzLogger.log(String.format("Deployment not found for requested Id %s", deploymentId), Severity.WARNING);
				return new ResponseEntity<>(
						new ErrorResponse(String.format("Deployment not found: %s", deploymentId), ACCESS_COMPONENT_NAME),
						HttpStatus.NOT_FOUND);
			}

			// Get the expiration date for this Deployment
			Lease lease = accessor.getDeploymentLease(deployment);
			String expiresOn = null;
			if (lease != null) {
				expiresOn = lease.getExpiresOn();
			}

			// Return the Data Resource item
			pzLogger.log(String.format("Returning Deployment Metadata for %s", deploymentId), Severity.INFORMATIONAL);
			return new ResponseEntity<>(new DeploymentResponse(deployment, expiresOn), HttpStatus.OK);
		} catch (Exception exception) {
			String error = String.format("Error fetching Deployment %s: %s", deploymentId, exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR, new AuditElement(ACCESS, "errorReadingDeploymentMetadata", deploymentId));
			return new ResponseEntity<>(new ErrorResponse("Error fetching Deployment: " + exception.getMessage(), ACCESS_COMPONENT_NAME),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Returns all Data held by the Piazza Ingest/Access components. This corresponds with the items in the Mongo
	 * db.Resources collection.
	 * 
	 * @return The list of all data held by the system.
	 */
	@RequestMapping(value = "/data", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> getAllData(@RequestParam(value = "createdByJobId", required = false) String createdByJobId,
			@RequestParam(value = "page", required = false, defaultValue = DEFAULT_PAGE) Integer page,
			@RequestParam(value = "perPage", required = false, defaultValue = DEFAULT_PAGE_SIZE) Integer pageSize,
			@RequestParam(value = "sortBy", required = false, defaultValue = DEFAULT_SORTBY) String sortBy,
			@RequestParam(value = "order", required = false, defaultValue = DEFAULT_ORDER) String order,
			@RequestParam(value = "keyword", required = false) String keyword,
			@RequestParam(value = "userName", required = false) String userName) {
		try {
			String orderToUse = order;
			// Don't allow for invalid orders
			if (!("asc".equalsIgnoreCase(order)) && !("desc".equalsIgnoreCase(order))) {
				orderToUse = "asc";
			}
			return new ResponseEntity<>(accessor.getDataList(page, pageSize, sortBy, orderToUse, keyword, userName, createdByJobId),
					HttpStatus.OK);
		} catch (Exception exception) {
			String error = String.format("Error Querying Data: %s", exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR, new AuditElement(ACCESS, "errorQueryingData", ""));
			return new ResponseEntity<>(new ErrorResponse("Error Querying Data: " + exception.getMessage(), ACCESS_COMPONENT_NAME),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Returns all Deployments held by the Piazza Ingest/Access components. This corresponds with the items in the Mongo
	 * db.Deployments collection.
	 * 
	 * @return The list of all data held by the system.
	 */
	@RequestMapping(value = "/deployment", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> getAllDeployments(
			@RequestParam(value = "page", required = false, defaultValue = DEFAULT_PAGE) Integer page,
			@RequestParam(value = "perPage", required = false, defaultValue = DEFAULT_PAGE_SIZE) Integer perPage,
			@RequestParam(value = "sortBy", required = false, defaultValue = DEFAULT_SORTBY) String sortBy,
			@RequestParam(value = "order", required = false, defaultValue = DEFAULT_ORDER) String order,
			@RequestParam(value = "keyword", required = false) String keyword) {
		try {
			String orderToUse = order;
			// Don't allow for invalid orders
			if (!("asc".equalsIgnoreCase(order)) && !("desc".equalsIgnoreCase(order))) {
				orderToUse = "asc";
			}
			return new ResponseEntity<>(accessor.getDeploymentList(page, perPage, sortBy, orderToUse, keyword), HttpStatus.OK);
		} catch (Exception exception) {
			String error = String.format("Error Querying Deployment: %s", exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR, new AuditElement(ACCESS, "errorReadingDeploymentList", ""));
			return new ResponseEntity<>(new ErrorResponse("Error Querying Deployment: " + exception.getMessage(), ACCESS_COMPONENT_NAME),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Returns the Number of Data Resources in the piazza system.
	 * 
	 * @return Number of Data items in the system.
	 */
	@RequestMapping(value = "/data/count", method = RequestMethod.GET)
	public long getDataCount() {
		return accessor.getDataCount();
	}

	/**
	 * Deletes a Deployment by it's Data ID. Internal method, should not be called by Gateway or other users. This is
	 * called by Ingest for clean-up when deleting Data items via DELETE /data/{dataId}
	 * 
	 * @param dataId
	 *            The Data ID to delete all deployments for.
	 * @return Response indicating true or false success of the deletion.
	 */
	@RequestMapping(value = "/deployment", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> deleteDeploymentByData(@RequestParam(value = "dataId", required = true) String dataId) {
		try {
			// Get the Deployment for this Data ID
			Deployment deployment = accessor.getDeploymentByDataId(dataId);

			if (deployment != null) {
				// If it exists, Delete it
				return deleteDeployment(deployment.getDeploymentId());
			} else {
				// If it doesn't exist, then simply return OK.
				return new ResponseEntity<>(
						new SuccessResponse(String.format("Deployment for Data ID %s does not exist. No action to take.", dataId),
								ACCESS_COMPONENT_NAME),
						HttpStatus.OK);
			}
		} catch (Exception exception) {
			String error = String.format("Error Deleting Deployment for Data ID %s : %s", dataId, exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR, new AuditElement(ACCESS, "errorReadingDataIdDeployments", dataId));
			return new ResponseEntity<>(new ErrorResponse(error, ACCESS_COMPONENT_NAME), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Deletes Deployment information for an active deployment.
	 * 
	 * @param deploymentId
	 *            The Id of the deployment to delete.
	 * @return OK confirmation if deleted, or an ErrorResponse if exceptions occur
	 */
	@RequestMapping(value = "/deployment/{deploymentId}", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> deleteDeployment(@PathVariable(value = "deploymentId") String deploymentId) {
		try {
			// Query for the Deployment Id
			Deployment deployment = accessor.getDeployment(deploymentId);
			if (deployment == null) {
				pzLogger.log(String.format("Deployment not found for requested Id %s", deploymentId), Severity.WARNING);
				return new ResponseEntity<>(
						new ErrorResponse(String.format("Deployment not found: %s", deploymentId), ACCESS_COMPONENT_NAME),
						HttpStatus.NOT_FOUND);
			}

			// Delete the Deployment
			deployer.undeploy(deploymentId);
			// Return OK
			return new ResponseEntity<>(
					new SuccessResponse("Deployment " + deploymentId + " was deleted successfully", ACCESS_COMPONENT_NAME), HttpStatus.OK);
		} catch (Exception exception) {
			String error = String.format("Error Deleting Deployment %s: %s", deploymentId, exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR, new AuditElement(ACCESS, "errorReadingDeploymentId", deploymentId));
			return new ResponseEntity<>(new ErrorResponse(error, ACCESS_COMPONENT_NAME), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Creates a new Deployment Group in the Piazza database. No accompanying GeoServer Layer Group will be created yet
	 * at this point; however a placeholder GUID is associated with this Deployment Group that will be used as the title
	 * of the eventual GeoServer Layer Group.
	 * 
	 * @param createdBy
	 *            The user who requests the creation
	 * @return The Deployment Group Response
	 */
	@RequestMapping(value = "/deployment/group", method = RequestMethod.POST, produces = "application/json")
	public ResponseEntity<PiazzaResponse> createDeploymentGroup(@RequestParam(value = "createdBy", required = true) String createdBy) {
		try {
			// Create a new Deployment Group
			DeploymentGroup deploymentGroup = groupDeployer.createDeploymentGroup(createdBy);
			return new ResponseEntity<>(new DeploymentGroupResponse(deploymentGroup), HttpStatus.CREATED);
		} catch (Exception exception) {
			String error = String.format("Error Creating DeploymentGroup for user %s : %s", createdBy, exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR, new AuditElement(ACCESS, "errorCreatingDeploymentGroup", ""));
			return new ResponseEntity<>(new ErrorResponse(error, ACCESS_COMPONENT_NAME), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Deletes a Deployment Group from Piazza, and from the corresponding GeoServer.
	 * 
	 * @param deploymentGroupId
	 *            The Id of the deployment Group to delete.
	 * @return Appropriate response
	 */
	@RequestMapping(value = "/deployment/group/{deploymentGroupId}", method = RequestMethod.DELETE, produces = "application/json")
	public ResponseEntity<PiazzaResponse> deleteDeploymentGroup(@PathVariable(value = "deploymentGroupId") String deploymentGroupId) {
		try {
			if ((deploymentGroupId == null) || (deploymentGroupId.isEmpty())) {
				return new ResponseEntity<>(new ErrorResponse("DeploymentGroup Id not specified.", ACCESS_COMPONENT_NAME),
						HttpStatus.BAD_REQUEST);
			}
			// Delete the DeploymentGroup from GeoServer, and remove it from
			// the Piazza DB persistence
			DeploymentGroup deploymentGroup = accessor.getDeploymentGroupById(deploymentGroupId);
			if (deploymentGroup == null) {
				return new ResponseEntity<>(new ErrorResponse("DeploymentGroup does not exist.", ACCESS_COMPONENT_NAME),
						HttpStatus.NOT_FOUND);
			}
			groupDeployer.deleteDeploymentGroup(deploymentGroup);
			return new ResponseEntity<>(new SuccessResponse("Group Deleted.", ACCESS_COMPONENT_NAME), HttpStatus.OK);
		} catch (HttpStatusCodeException httpException) {
			// Return the HTTP Error code from GeoServer
			String error = String.format("Could not delete DeploymentGroup. Response from GeoServer returned code %s with reason %s",
					httpException.getStatusCode().toString(), httpException.getMessage());
			LOGGER.error(error, httpException);
			pzLogger.log(error, Severity.ERROR, new AuditElement(ACCESS, "errorDeletingDeploymentGroup", deploymentGroupId));
			return new ResponseEntity<>(new ErrorResponse(error, ACCESS_COMPONENT_NAME), httpException.getStatusCode());
		} catch (Exception exception) {
			// Return the 500 Internal error
			String error = String.format("Could not delete DeploymentGroup. An unexpected error occurred: %s", exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR, new AuditElement(ACCESS, "errorDeletingDeploymentGroup", deploymentGroupId));
			return new ResponseEntity<>(new ErrorResponse(error, ACCESS_COMPONENT_NAME), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Forces a check of all expired leases for reaping. Reaping will normally occur automatically every night. However,
	 * this endpoint provides a way to trigger at will.
	 */
	@RequestMapping(value = "/reap", method = RequestMethod.GET)
	public void forceReap() {
		leaser.reapExpiredLeases();
	}

	/**
	 * Returns administrative statistics for this component.
	 * 
	 * @return Component information
	 */
	@RequestMapping(value = "/admin/stats", method = RequestMethod.GET)
	public ResponseEntity<Map<String, Object>> getAdminStats() {
		Map<String, Object> stats = new HashMap<>();
		// Return information on the jobs currently being processed
		stats.put("jobs", threadManager.getRunningJobIds());
		stats.put("activeThreads", threadPoolTaskExecutor.getActiveCount());
		if (threadPoolTaskExecutor.getThreadPoolExecutor() != null) {
			stats.put("threadQueue", threadPoolTaskExecutor.getThreadPoolExecutor().getQueue().size());
		}
		return new ResponseEntity<>(stats, HttpStatus.OK);
	}

	/**
	 * @param type
	 *            MediaType to set http header content type
	 * @param fileName
	 *            file name to set for content disposition
	 * @param bytes
	 *            file bytes
	 * @return ResponseEntity
	 */
	private ResponseEntity<byte[]> getResponse(MediaType type, String fileName, byte[] bytes) {
		HttpHeaders header = new HttpHeaders();
		header.setContentType(type);
		header.set("Content-Disposition", "attachment; filename=" + fileName);
		header.setContentLength(bytes.length);
		return new ResponseEntity<>(bytes, header, HttpStatus.OK);
	}

	/**
	 * Gets the GeoJSON representation of a Data Resource currently stored in PostGIS.
	 * 
	 * @param data
	 *            DataResource object
	 * @return stringbuilder of geojson
	 * @throws Exception
	 */
	private StringBuilder getPostGISGeoJSON(DataResource data) throws IOException {
		// Connect to POSTGIS and gather geoJSON info
		DataStore postGisStore = accessor.getPostGisDataStore(postgresHost, postgresPort, postgresSchema, postgresDBName, postgresUser,
				postgresPassword);

		PostGISDataType resource = (PostGISDataType) (data.getDataType());
		SimpleFeatureSource simpleFeatureSource = postGisStore.getFeatureSource(resource.getTable());
		SimpleFeatureCollection simpleFeatureCollection = simpleFeatureSource.getFeatures(Query.ALL);

		StringBuilder geoJSON = new StringBuilder();
		try (SimpleFeatureIterator simpleFeatureIterator = simpleFeatureCollection.features()) {
			while (simpleFeatureIterator.hasNext()) {
				SimpleFeature simpleFeature = simpleFeatureIterator.next();
				FeatureJSON featureJSON = new FeatureJSON();
				StringWriter writer = new StringWriter();

				featureJSON.writeFeature(simpleFeature, writer);
				String json = writer.toString();

				// Append each section
				geoJSON.append(json);
			}
		}

		return geoJSON;
	}
}
