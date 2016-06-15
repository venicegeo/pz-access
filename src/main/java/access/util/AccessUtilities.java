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

import model.data.location.S3FileStore;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import util.PiazzaLogger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * Utility class to handle common functionality required by access components
 * 
 * @author Patrick.Doody
 *
 */
@Component
public class AccessUtilities {
	@Autowired
	private PiazzaLogger logger;
	@Value("${vcap.services.pz-geoserver.credentials.s3.access_key_id}")
	private String AMAZONS3_ACCESS_KEY;
	@Value("${vcap.services.pz-geoserver.credentials.s3.secret_access_key}")
	private String AMAZONS3_PRIVATE_KEY;
	@Value("${vcap.services.pz-geoserver.credentials.s3.bucket}")
	private String GEOSERVER_DATA_DIRECTORY;

	/**
	 * Copies the S3FileStore object to the GeoServer data directory with the
	 * specified destination file name.
	 * 
	 * @param fileStore
	 *            The S3FileStore of a DataResource object to copy to
	 *            GeoServer's data dir
	 * @param destinationFileName
	 *            The name of the key that the new file will be copied to
	 */
	public void copyS3FileStoreToGeserver(S3FileStore fileStore, String destinationFileName) {
		// Get AWS Client
		AmazonS3 s3client = new AmazonS3Client(new BasicAWSCredentials(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY));
		// Copy the file to the GeoServer S3 Bucket
		logger.log(
				String.format(
						"Preparing to deploy Raster service. Moving file %s:%s from Piazza bucket into GeoServer bucket at %s:%s",
						fileStore.getBucketName(), fileStore.getFileName(), GEOSERVER_DATA_DIRECTORY,
						destinationFileName), PiazzaLogger.INFO);

		s3client.copyObject(fileStore.getBucketName(), fileStore.getFileName(), GEOSERVER_DATA_DIRECTORY,
				destinationFileName);
	}
}
