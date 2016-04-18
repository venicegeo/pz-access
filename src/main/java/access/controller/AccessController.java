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

import java.io.InputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import model.data.DataResource;
import model.data.FileRepresentation;
import model.data.location.FileAccessFactory;
import model.data.type.PostGISDataType;
import model.data.type.TextDataType;
import model.response.DataResourceListResponse;
import model.response.DataResourceResponse;
import model.response.ErrorResponse;
import model.response.Pagination;
import model.response.PiazzaResponse;

import org.geotools.data.DataStore;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geojson.feature.FeatureJSON;
import org.mongojack.DBCursor;
import org.opengis.feature.simple.SimpleFeature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import util.GeoToolsUtil;
import util.PiazzaLogger;
import access.database.MongoAccessor;
import access.messaging.AccessThreadManager;

/**
 * Allows for synchronous fetching of Resource Data from the Mongo Resource
 * collection.
 * 
 * The collection is bound to the DataResource model.
 * 
 * This controller is similar to the functionality of the JobManager REST
 * Controller, in that this component primarily listens for messages via Kafka,
 * however, for instances where the user needs a direct read out of the database
 * - this should be a synchronous response that does not involve Kafka. For such
 * requests, this REST controller exists.
 * 
 * @author Patrick.Doody
 * 
 */
@RestController
public class AccessController {

	@Value("${vcap.services.pz-postgres.credentials.host}")
	private String POSTGRES_HOST;
	@Value("${vcap.services.pz-postgres.credentials.port}")
	private String POSTGRES_PORT;
	@Value("${vcap.services.pz-postgres.credentials.database}")
	private String POSTGRES_DB_NAME;
	@Value("${vcap.services.pz-postgres.credentials.username}")
	private String POSTGRES_USER;
	@Value("${vcap.services.pz-postgres.credentials.password}")
	private String POSTGRES_PASSWORD;
	@Value("${postgres.schema}")
	private String POSTGRES_SCHEMA;

	@Autowired
	private AccessThreadManager threadManager;
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private MongoAccessor accessor;

	@Value("${vcap.services.pz-blobstore.credentials.access:}")
	private String AMAZONS3_ACCESS_KEY;
	@Value("${vcap.services.pz-blobstore.credentials.private:}")
	private String AMAZONS3_PRIVATE_KEY;

	private static final String DEFAULT_PAGE_SIZE = "10";
	private static final String DEFAULT_PAGE = "0";

	/**
	 * Requests a file download that has been prepared by this Access component.
	 * This will return the raw bytes of the resource.
	 * 
	 * @param dataId
	 *            The ID of the Data Item to get. Assumes this file is ready to
	 *            be downloaded.
	 */
	@RequestMapping(value = "/file/{dataId}", method = RequestMethod.GET)
	public ResponseEntity<byte[]> accessFile(@PathVariable(value = "dataId") String dataId) throws Exception {
		// Get the DataResource item
		DataResource data = accessor.getData(dataId);
		if (data == null) {
			String message = String.format("Data not found for requested ID %s", dataId);
			logger.log(message, PiazzaLogger.WARNING);
			throw new Exception(message);
		}

		StringBuilder geoJSON = new StringBuilder();
		if (data.getDataType() instanceof TextDataType) {
			// Stream the Bytes back
			TextDataType textData = (TextDataType) data.getDataType();
			HttpHeaders header = new HttpHeaders();
			header.setContentType(MediaType.TEXT_PLAIN);
			header.set("Content-Disposition", "attachment; filename=" + dataId + ".txt");
			header.setContentLength(textData.getContent().getBytes().length);
			return new ResponseEntity<byte[]>(textData.getContent().getBytes(), header, HttpStatus.OK);
		} else if (data.getDataType() instanceof PostGISDataType) {
			// Connect to POSTGIS and gather geoJSON info
			DataStore postGisStore = GeoToolsUtil.getPostGisDataStore(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_SCHEMA,
					POSTGRES_DB_NAME, POSTGRES_USER, POSTGRES_PASSWORD);

			PostGISDataType resource = (PostGISDataType) (data.getDataType());
			SimpleFeatureSource simpleFeatureSource = postGisStore.getFeatureSource(resource.getTable());
			SimpleFeatureCollection simpleFeatureCollection = simpleFeatureSource.getFeatures(Query.ALL);
			SimpleFeatureIterator simpleFeatureIterator = simpleFeatureCollection.features();

			try {
				while (simpleFeatureIterator.hasNext()) {
					SimpleFeature simpleFeature = simpleFeatureIterator.next();
					FeatureJSON featureJSON = new FeatureJSON();
					StringWriter writer = new StringWriter();

					featureJSON.writeFeature(simpleFeature, writer);
					String json = writer.toString();

					// Append each section
					geoJSON.append(json);
				}
			} finally {
				simpleFeatureIterator.close();
			}

			// Log the Request
			logger.log(String.format("Returning Bytes for %s of length %s", dataId, geoJSON.length()),
					PiazzaLogger.INFO);

			// Stream the Bytes back
			HttpHeaders header = new HttpHeaders();
			header.setContentType(MediaType.TEXT_PLAIN);
			header.set("Content-Disposition",
					"attachment; filename=" + ((PostGISDataType) data.getDataType()).getTable());
			header.setContentLength(geoJSON.length());
			return new ResponseEntity<byte[]>(geoJSON.toString().getBytes(), header, HttpStatus.OK);
		} else if (!(data.getDataType() instanceof FileRepresentation)) {
			String message = String.format("File download not available for Data ID %s; type is %s", dataId, data
					.getDataType().getType());
			logger.log(message, PiazzaLogger.WARNING);
			throw new Exception(message);
		} else {
			// Get the File Bytes from wherever the File Location
			FileAccessFactory fileFactory = new FileAccessFactory(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY);
			InputStream byteStream = fileFactory.getFile(((FileRepresentation) data.getDataType()).getLocation());
			byte[] bytes = StreamUtils.copyToByteArray(byteStream);

			// Log the Request
			logger.log(String.format("Returning Bytes for %s of length %s", dataId, bytes.length), PiazzaLogger.INFO);

			// Get the file name.
			String fileName = ((FileRepresentation) data.getDataType()).getLocation().getFileName();
			// Strip out the Job ID GUID in the file name.
			if (fileName.length() > 37) {
				fileName = fileName.substring(37, fileName.length());
			}

			// Stream the Bytes back
			HttpHeaders header = new HttpHeaders();
			header.setContentType(MediaType.APPLICATION_OCTET_STREAM);
			header.set("Content-Disposition", "attachment; filename=" + fileName);
			header.setContentLength(bytes.length);
			return new ResponseEntity<byte[]>(bytes, header, HttpStatus.OK);
		}
	}

	/**
	 * Returns the Data resource object from the Resources collection.
	 * 
	 * @param dataId
	 *            ID of the Resource
	 * @return The resource matching the specified ID
	 */
	@RequestMapping(value = "/data/{dataId}", method = RequestMethod.GET)
	public PiazzaResponse getData(@PathVariable(value = "dataId") String dataId) {
		try {
			if (dataId.isEmpty()) {
				throw new Exception("No Data ID specified.");
			}
			// Query for the Data ID
			DataResource data = accessor.getData(dataId);
			if (data == null) {
				logger.log(String.format("Data not found for requested ID %s", dataId), PiazzaLogger.WARNING);
				return new ErrorResponse(null, String.format("Data not found: %s", dataId), "Access");
			}

			// Return the Data Resource item
			logger.log(String.format("Returning Data Metadata for %s", dataId), PiazzaLogger.INFO);
			return new DataResourceResponse(data);
		} catch (Exception exception) {
			exception.printStackTrace();
			logger.log(String.format("Error fetching Data %s: %s", dataId, exception.getMessage()), PiazzaLogger.ERROR);
			return new ErrorResponse(null, "Error fetching Data: " + exception.getMessage(), "Access");
		}
	}

	/**
	 * Returns all Data held by the Piazza Ingest/Access components. This
	 * corresponds with the items in the Mongo db.Resources collection.
	 * 
	 * This is intended to be used by the Swiss-Army-Knife (SAK) administration
	 * application for reporting the status of this Job Manager component. It is
	 * not used in normal function of the Job Manager.
	 * 
	 * @return The list of all data held by the system.
	 */
	@RequestMapping(value = "/data", method = RequestMethod.GET)
	public DataResourceListResponse getAllData(
			@RequestParam(value = "page", required = false, defaultValue = DEFAULT_PAGE) Integer page,
			@RequestParam(value = "per_page", required = false, defaultValue = DEFAULT_PAGE_SIZE) Integer pageSize) {
		// Get a DB Cursor to the query for general data
		DBCursor<DataResource> cursor = accessor.getDataResourceCollection().find();
		Integer size = new Integer(cursor.size());
		// Filter the data by pages
		List<DataResource> data = cursor.skip(page * pageSize).limit(pageSize).toArray();
		// Attach pagination information
		Pagination pagination = new Pagination(size, page, pageSize);
		// Create the Response and send back
		return new DataResourceListResponse(data, pagination);
	}

	/**
	 * Returns the Number of Data Resources in the piazza system.
	 * 
	 * This is intended to be used by the Swiss-Army-Knife (SAK) administration
	 * application for reporting the status of this Job Manager component. It is
	 * not used in normal function of the Job Manager.
	 * 
	 * @return Number of Data items in the system.
	 */
	@RequestMapping(value = "/data/count", method = RequestMethod.GET)
	public long getDataCount() {
		return accessor.getDataResourceCollection().count();
	}

	/**
	 * Drops the Mongo collections. This is for internal development use only.
	 * We should probably remove this in the future. Don't use this.
	 */
	@RequestMapping(value = "/drop")
	public String dropAllTables(@RequestParam(value = "serious", required = false) Boolean serious) {
		if ((serious != null) && (serious.booleanValue())) {
			accessor.getDataResourceCollection().drop();
			accessor.getLeaseCollection().drop();
			accessor.getDeploymentCollection().drop();
			return "Collections dropped.";
		} else {
			return "You're not serious.";
		}
	}

	/**
	 * Returns administrative statistics for this component.
	 * 
	 * @return Component information
	 */
	@RequestMapping(value = "/admin/stats", method = RequestMethod.GET)
	public ResponseEntity<Map<String, Object>> getAdminStats() {
		Map<String, Object> stats = new HashMap<String, Object>();
		// Return information on the jobs currently being processed
		stats.put("jobs", threadManager.getRunningJobIDs());
		return new ResponseEntity<Map<String, Object>>(stats, HttpStatus.OK);
	}
}
