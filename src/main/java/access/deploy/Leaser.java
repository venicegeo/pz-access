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

import java.util.UUID;

import model.data.deployment.Deployment;
import model.data.deployment.Lease;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import access.database.MongoAccessor;

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
	@Autowired
	private MongoAccessor accessor;
	private static final int DEFAULT_LEASE_PERIOD_DAYS = 7;

	/**
	 * Renews the existing Deployment. This Deployment must exist in the
	 * Deployments collection.
	 * 
	 * @param deployment
	 *            The deployment to renew.
	 * @return The Lease for this Deployment
	 */
	public Lease renewDeploymentLease(Deployment deployment) {
		Lease lease = accessor.getDeploymentLease(deployment);
		// If the lease has been reaped by the database, then create a new
		// Lease.
		if (lease == null) {
			lease = createDeploymentLease(deployment);
		} else {
			DateTime expirationDate = new DateTime(lease.getExpirationDate());
			if (expirationDate.isBeforeNow()) {
				// If the Lease has expired, then the Lease will be extended for
				// the default Lease period.
				accessor.updateLeaseExpirationDate(lease.getId(), DateTime.now().plusDays(DEFAULT_LEASE_PERIOD_DAYS)
						.toString());
			} else {
				// If the Lease has not expired, then the Lease will not be
				// extended. It will simply be reused.
			}
		}

		return lease;
	}

	/**
	 * Creates a new lease for the Deployment.
	 * 
	 * @param deployment
	 *            Deployment to create a lease for
	 */
	public Lease createDeploymentLease(Deployment deployment) {
		// Create the Lease
		String leaseId = UUID.randomUUID().toString();
		Lease lease = new Lease(leaseId, deployment.getId(), DateTime.now().plusDays(DEFAULT_LEASE_PERIOD_DAYS)
				.toString());

		// Commit the Lease to the Database
		accessor.insertLease(lease);

		// Return reference
		return lease;
	}
}
