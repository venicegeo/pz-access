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
package access.deploy;

import org.springframework.stereotype.Component;

/**
 * Handles the accessing of Resource files for deployment. When a Resource is to
 * be deployed, often times resources must be collected, either from disk or S3,
 * or some other data store. This class acts to facilitate the access of those
 * files so that they may be made accessible to components such as GeoServer.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class ResourceAccessor {

}
