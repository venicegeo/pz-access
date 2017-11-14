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
package access.util;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.AmazonClientException;

import exception.InvalidInputException;
import model.data.DataResource;
import model.data.FileRepresentation;
import model.data.location.FileAccessFactory;
import model.data.location.FileLocation;
import model.data.location.S3FileStore;
import model.logger.AuditElement;
import model.logger.Severity;
import util.PiazzaLogger;

/**
 * Utility class to handle common functionality required by access components
 * 
 * @author Patrick.Doody
 *
 */
@Component
public class AccessUtilities {
	@Value("${vcap.services.pz-geoserver.credentials.boundless_geoserver_url}")
	private String geoserverUrl;
	@Value("${vcap.services.pz-blobstore.credentials.access_key_id}")
	private String amazonS3AccessKey;
	@Value("${vcap.services.pz-blobstore.credentials.secret_access_key}")
	private String amazonS3PrivateKey;
	@Value("${vcap.services.pz-blobstore.credentials.bucket:}")
	private String PIAZZA_BUCKET;
	@Value("${vcap.services.pz-blobstore.credentials.encryption_key}")
	private String S3_KMS_CMK_ID;
	@Value("${s3.use.kms}")
	private Boolean USE_KMS;
	@Autowired
	private PiazzaLogger logger;

	/**
	 * Gets the base GeoServer URL.
	 * <p>
	 * The GeoServer URL as reported by On-Demand service tile is something along the lines of:
	 * "https://geoserver.ip/geoserver/index.html". In our requests to GeoServer, the index.html portion of the URL is
	 * obstructing. This method will take in the current VCAP variable for the Boundless URL and return the URL base
	 * (leading up to the /geoserver path) that can be used to construct URLs for proper POST/PUTs to GeoServer.
	 * </p>
	 * <p>
	 * The return value will take on the form of "http://geoserver.ip/geoserver". Protocol will be determined by the
	 * VCAP variable. Port will be included if defined in the VCAP, otherwise it will not be present. There will be no
	 * trailing slash at the end of the URL.
	 * </p>
	 */
	public String getGeoServerBaseUrl() {
		String baseUrl = geoserverUrl;
		if (geoserverUrl.contains("/index.html")) {
			baseUrl = geoserverUrl.replace("/index.html", "");
		}
		return baseUrl;
	}

	/**
	 * Gets the Bytes for a Data Resource
	 * 
	 * @param dataResource
	 *            The Data Resource
	 * @return The byte array for the file
	 * @throws InvalidInputException
	 * @throws AmazonClientException
	 * @throws Exception
	 */
	public byte[] getBytesForDataResource(DataResource dataResource) throws IOException, InvalidInputException {
		logger.log("Fetching Bytes for Data Item", Severity.INFORMATIONAL,
				new AuditElement("access", "getBytesForData", dataResource.getDataId()));
		FileLocation fileLocation = ((FileRepresentation) dataResource.getDataType()).getLocation();
		FileAccessFactory fileAccessFactory = getFileFactoryForDataResource(dataResource);
		return IOUtils.toByteArray(fileAccessFactory.getFile(fileLocation));
	}

	/**
	 * Returns an instance of the File Factory, instantiated with the correct credentials for the use of obtaining file
	 * bytes for the specified Data Resource. Such as if the Resource is a file, or an S3 Bucket, or an Encrypted S3
	 * bucket. TODO: Move this into JobCommon
	 * 
	 * @param dataResource
	 *            The Data Resource
	 * @return FileAccessFactory
	 */
	public FileAccessFactory getFileFactoryForDataResource(DataResource dataResource) {
		// If S3 store, determine if this is the Piazza bucket (use encryption) or not (dont use encryption)
		final FileAccessFactory fileFactory;
		FileLocation fileLocation = ((FileRepresentation) dataResource.getDataType()).getLocation();
		if (fileLocation instanceof S3FileStore) {
			if (PIAZZA_BUCKET.equals(((S3FileStore) fileLocation).getBucketName())) {
				// Use encryption if enabled
				if (USE_KMS.booleanValue()) {
					fileFactory = new FileAccessFactory(amazonS3AccessKey, amazonS3PrivateKey, S3_KMS_CMK_ID);
				} else {
					fileFactory = new FileAccessFactory(amazonS3AccessKey, amazonS3PrivateKey);
				}
			} else {
				fileFactory = new FileAccessFactory(amazonS3AccessKey, amazonS3PrivateKey);
			}
		} else {
			// No AWS Creds needed
			fileFactory = new FileAccessFactory();
		}
		return fileFactory;
	}
}
