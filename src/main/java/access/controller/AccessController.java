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

import model.data.DataResource;
import model.response.DataResourceResponse;
import model.response.ErrorResponse;
import model.response.PiazzaResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.ResourceAccessException;

import access.database.MongoAccessor;

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
	@Autowired
	private MongoAccessor accessor;

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
			// Return the Data Resource item
			return new DataResourceResponse(data);
		} catch (ResourceAccessException exception) {
			return new ErrorResponse(null, exception.getMessage(), "Access");
		} catch (Exception exception) {
			exception.printStackTrace();
			return new ErrorResponse(null, "Error fetching Data: " + exception.getMessage(), "Access");
		}
	}
}
