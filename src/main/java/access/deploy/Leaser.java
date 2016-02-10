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

import access.database.model.Deployment;
import access.database.model.Lease;

/**
 * Handles the leasing of deployments of Resources. When a Resource is deployed,
 * it is assigned a lease for a certain period of time. Leasing allows Piazza
 * resources to be managed over time, so that deployments do not live forever
 * and potentially clog up resources. Leases are created when Access Jobs are
 * processed. If a Deployment runs out of a lease, it is vulnerable to being
 * cleaned up by resource reaping.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class Leaser {

	/**
	 * Renews the existing deployment.
	 * 
	 * @param deployment
	 *            The deployment to renew.
	 */
	public void renewDeploymentLease(Deployment deployment) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Creates a new lease for the Deployment.
	 * 
	 * @param deployment
	 *            Deployment to create a lease for
	 */
	public Lease getDeploymentLease(Deployment deployment) {
		throw new UnsupportedOperationException();
	}
}
