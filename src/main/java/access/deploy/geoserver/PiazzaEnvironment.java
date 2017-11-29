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
package access.deploy.geoserver;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import access.util.AccessUtilities;
import model.logger.AuditElement;
import model.logger.Severity;
import util.PiazzaLogger;

/**
 * Establishes a Piazza environment on the GeoServer during initialization to ensure that prerequisite Workspaces and
 * Data Stores exist on the server before use
 * 
 * @author Patrick.Doody
 *
 */
@Component
public class PiazzaEnvironment {
	@Value("${vcap.services.pz-postgres.credentials.db_host}")
	private String postgresHost;
	@Value("${vcap.services.pz-postgres.credentials.db_port}")
	private String postgresPort;
	@Value("${vcap.services.pz-postgres.credentials.db_name}")
	private String postgresDatabase;
	@Value("${vcap.services.pz-postgres.credentials.username}")
	private String postgresUser;
	@Value("${vcap.services.pz-postgres.credentials.password}")
	private String postgresPassword;
	@Value("${vcap.services.pz-postgres-service-key.credentials.username}")
	private String postgresServiceKeyUser;
	@Value("${vcap.services.pz-postgres-service-key.credentials.password}")
	private String postgresServiceKeyPassword;
	@Autowired
	private RestTemplate restTemplate;
	@Autowired
	private AuthHeaders authHeaders;
	@Autowired
	private PiazzaLogger pzLogger;
	@Autowired
	private AccessUtilities accessUtilities;

	private static final Logger LOGGER = LoggerFactory.getLogger(PiazzaEnvironment.class);
	private static final String ACCESS = "access";

	/**
	 * Invokes initialization logic for Piazza workspace and PostGIS Data Store
	 */
	@PostConstruct
	public void initializeEnvironment() {
		pzLogger.log("Initializing - checking GeoServer for required workspaces and data stores.", Severity.INFORMATIONAL);

		// Check for Workspace
		try {
			String workspaceUri = String.format("%s/rest/workspaces/piazza.json", accessUtilities.getGeoServerBaseUrl());
			if (!doesResourceExist(workspaceUri)) {
				createWorkspace();
			} else {
				LOGGER.info("GeoServer Piazza Workspace found.");
			}
		} catch (Exception exception) {
			String error = "Server error encountered while trying to check Piazza Workspace. Will not attempt to create this Resource.";
			LOGGER.warn(error, exception);
			pzLogger.log(error, Severity.WARNING);
		}

		// Check for Data Store
		try {
			String dataStoreUri = String.format("%s/rest/workspaces/piazza/datastores/piazza.json", accessUtilities.getGeoServerBaseUrl());
			if (!doesResourceExist(dataStoreUri)) {
				createPostgresStore();
			} else {
				LOGGER.info("GeoServer Piazza Data Store found.");
			}
		} catch (Exception exception) {
			String error = "Server error encountered while trying to check Piazza Data Store. Will not attempt to create this Resource.";
			LOGGER.warn(error, exception);
			pzLogger.log(error, Severity.WARNING);
		}
	}

	/**
	 * Checks if a GeoServer resource exists (200 OK returns from the server. 404 indicates not exists)
	 * 
	 * @return True if exists, false if not
	 */
	private boolean doesResourceExist(String resourceUri) throws HttpStatusCodeException, RestClientException {
		// Check if exists
		HttpEntity<String> request = new HttpEntity<>(authHeaders.get());

		try {
			pzLogger.log(String.format("Checking if GeoServer Resource Exists at %s", resourceUri), Severity.INFORMATIONAL,
					new AuditElement(ACCESS, "checkGeoServerResourceExists", resourceUri));
			ResponseEntity<String> response = restTemplate.exchange(resourceUri, HttpMethod.GET, request, String.class);
			// Validate that the response body is JSON. Otherwise, an authentication redirect error may have occurred.
			ObjectMapper objectMapper = new ObjectMapper();
			try {
				objectMapper.readTree(response.getBody());
			} catch (IOException exception) {
				String error = String.format(
						"Received a non-error response from GeoServer resource check for %s, but it was not valid JSON. Authentication may have failed.",
						resourceUri);
				LOGGER.error(error, exception);
				pzLogger.log(error, Severity.ERROR);
				return false;
			}
			if (response.getStatusCode().is2xxSuccessful()) {
				return true;
			}
		} catch (HttpClientErrorException | HttpServerErrorException exception) {
			// If it's a 404, then it does not exist. Fall through.
			if (!exception.getStatusCode().equals(HttpStatus.NOT_FOUND)) {
				// If it's anything but a 404, then it's a server error and we should not proceed with creation. Throw
				// an exception.
				LOGGER.error("HTTP Status Error checking GeoServer Resource {} Exists : {}" + resourceUri,
						exception.getStatusCode().toString(), exception);
				throw exception;
			}
		} catch (RestClientException exception) {
			LOGGER.error("Unexpected Error Checking GeoServer Resource Exists : " + resourceUri, exception);
			throw exception;
		}
		return false;
	}

	/**
	 * Creates the Piazza workspace
	 */
	private void createWorkspace() {
		// POST the Workspace
		authHeaders.setContentType(MediaType.APPLICATION_XML);
		String body = "<workspace><name>piazza</name></workspace>";
		HttpEntity<String> request = new HttpEntity<>(body, authHeaders.get());
		String uri = String.format("%s/rest/workspaces", accessUtilities.getGeoServerBaseUrl());
		try {
			pzLogger.log(String.format("Creating Piazza Workspace to %s", uri), Severity.INFORMATIONAL,
					new AuditElement(ACCESS, "tryCreateGeoServerWorkspace", uri));
			restTemplate.exchange(uri, HttpMethod.POST, request, String.class);
		} catch (HttpClientErrorException | HttpServerErrorException exception) {
			String error = String.format("HTTP Error occurred while trying to create Piazza Workspace: %s",
					exception.getResponseBodyAsString());
			LOGGER.info(error, exception);
			pzLogger.log(error, Severity.WARNING);
		} catch (Exception exception) {
			String error = String.format("Unexpected Error occurred while trying to create Piazza Workspace: %s", exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR);
		}
	}

	/**
	 * Creates the Piazza Postgres vector data store
	 */
	private void createPostgresStore() {
		// Get Request XML
		ClassLoader classLoader = getClass().getClassLoader();
		InputStream inputStream = null;
		String dataStoreBody = null;
		try {
			inputStream = classLoader.getResourceAsStream("templates" + File.separator + "createDataStore.xml");
			dataStoreBody = IOUtils.toString(inputStream);
		} catch (Exception exception) {
			LOGGER.error("Error reading GeoServer Data Store Template.", exception);
		} finally {
			try {
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (Exception exception) {
				LOGGER.error("Error closing GeoServer Data Store Template Stream.", exception);
			}
		}
		// Create Workspace
		if (dataStoreBody != null) {
			// Insert the credential data into the template
			dataStoreBody = dataStoreBody.replace("$DB_USER", postgresServiceKeyUser);
			dataStoreBody = dataStoreBody.replace("$DB_PASSWORD", postgresServiceKeyPassword);
			dataStoreBody = dataStoreBody.replace("$DB_PORT", postgresPort);
			dataStoreBody = dataStoreBody.replace("$DB_NAME", postgresDatabase);
			dataStoreBody = dataStoreBody.replace("$DB_HOST", postgresHost);

			// POST Data Store to GeoServer
			authHeaders.setContentType(MediaType.APPLICATION_XML);
			HttpEntity<String> request = new HttpEntity<>(dataStoreBody, authHeaders.get());
			String uri = String.format("%s/rest/workspaces/piazza/datastores", accessUtilities.getGeoServerBaseUrl());
			try {
				pzLogger.log(String.format("Creating Piazza Data Store to %s", uri), Severity.INFORMATIONAL,
						new AuditElement(ACCESS, "tryCreateGeoServerDataStore", uri));
				restTemplate.exchange(uri, HttpMethod.POST, request, String.class);
			} catch (HttpClientErrorException | HttpServerErrorException exception) {
				String error = String.format("HTTP Error occurred while trying to create Piazza Data Store: %s",
						exception.getResponseBodyAsString());
				LOGGER.info(error, exception);
				pzLogger.log(error, Severity.WARNING);
			} catch (Exception exception) {
				String error = String.format("Unexpected Error occurred while trying to create Piazza Data Store: %s",
						exception.getMessage());
				LOGGER.error(error, exception);
				pzLogger.log(error, Severity.ERROR);
			}
		} else {
			pzLogger.log("Could not create GeoServer Data Store. Could not load Request XML from local Resources.", Severity.ERROR);
		}
	}
}
