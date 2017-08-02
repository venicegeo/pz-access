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

import org.joda.time.DateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import access.database.DatabaseAccessor;
import model.data.deployment.Deployment;
import model.data.deployment.Lease;
import model.logger.AuditElement;
import model.logger.Severity;
import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * Handles the leasing of deployments of Resources. When a Resource is deployed, it is assigned a lease for a certain
 * period of time. Leasing allows Piazza resources to be managed over time, so that deployments do not live forever and
 * potentially clog up resources. Leases are created when Access Jobs are processed. If a Deployment runs out of a
 * lease, it is vulnerable to being cleaned up by resource reaping.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class Leaser {
	@Autowired
	private Deployer deployer;
	@Autowired
	private PiazzaLogger pzLogger;
	@Autowired
	private UUIDFactory uuidFactory;
	@Autowired
	private DatabaseAccessor accessor;
	
	private static final Integer DEFAULT_LEASE_PERIOD_DAYS = 21;

	private static final Logger LOGGER = LoggerFactory.getLogger(Leaser.class);
	private static final String ACCESS = "access";

	/**
	 * Renews the existing Deployment. This Deployment must exist in the Deployments collection.
	 * 
	 * @param deployment
	 *            The deployment to renew.
	 * @param durationDays
	 *            The number of days to renew the lease by.
	 * @return The Lease for this Deployment
	 */
	public Lease renewDeploymentLease(Deployment deployment, Integer durationDays) {
		Lease lease = accessor.getDeploymentLease(deployment);
		// If the lease has been reaped by the database, then create a new
		// Lease.
		if (lease == null) {
			lease = createDeploymentLease(deployment, durationDays);
		} else {
			DateTime expirationDate = new DateTime(lease.getExpiresOn());
			if (expirationDate.isBeforeNow()) {
				// If the Lease has expired, then the Lease will be extended for
				// the default Lease period.
				Integer updatedDurationDays = ((durationDays != null) && (durationDays.intValue() > 0)) ? durationDays
						: DEFAULT_LEASE_PERIOD_DAYS;
				accessor.updateLeaseExpirationDate(lease.getLeaseId(), DateTime.now().plusDays(updatedDurationDays.intValue()).toString());
				pzLogger.log(
						String.format("Updating Deployment Lease for Deployment %s on host %s for %s", deployment.getDeploymentId(),
								deployment.getHost(), deployment.getDataId()),
						Severity.INFORMATIONAL, new AuditElement(ACCESS, "renewDeploymentLease", deployment.getDeploymentId()));
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
	 * @param durationDays
	 *            the Number of days to create the deployment for
	 */
	public Lease createDeploymentLease(Deployment deployment, Integer durationDays) {
		// Create the Lease
		String leaseId = uuidFactory.getUUID();
		Integer updatedDurationDays = ((durationDays != null) && (durationDays.intValue() > 0)) ? durationDays : DEFAULT_LEASE_PERIOD_DAYS;
		Lease lease = new Lease(leaseId, deployment.getDeploymentId(), DateTime.now().plusDays(updatedDurationDays.intValue()).toString());

		// Commit the Lease to the Database
		accessor.insertLease(lease);

		// Return reference
		pzLogger.log(
				String.format("Creating Deployment Lease for Deployment %s on host %s for %s", deployment.getDeploymentId(),
						deployment.getHost(), deployment.getDataId()),
				Severity.INFORMATIONAL, new AuditElement(ACCESS, "createDeploymentLease", leaseId));
		return lease;
	}

	/**
	 * <p>
	 * This method is scheduled to run periodically and look for leases that are expired. If a lease is found to be
	 * expired, and GeoServer resources are limited, then the lease will be terminated.
	 * </p>
	 * 
	 * <p>
	 * Leases might not be terminated if they are expired, if GeoServer has more than adequate resources available. This
	 * is configurable, but ultimately the goal is to create a friendly user experience while not bogging down GeoServer
	 * with lots of old, unused deployments.
	 * </p>
	 * 
	 * <p>
	 * This will currently run every day at 3:00am.
	 * </p>
	 */
	@Scheduled(cron = "0 0 3 * * ?")
	public void reapExpiredLeases() {
		// Log the initiation of reaping.
		pzLogger.log("Running scheduled daily reaping of expired Deployment Leases.", Severity.INFORMATIONAL);

		// Determine if GeoServer is reaching capacity of its resources.
		// TODO: Not sure if this is needed just yet.
		pzLogger.log("GeoServer not at capacity. No reaping of resources required.", Severity.INFORMATIONAL);

//		accessor.getExpiredLeases(DateTime.now());
//		
//		// Query for all leases that have gone past their expiration date.
//		BasicDBObject query = new BasicDBObject("expirationDate", new BasicDBObject("$lt", DateTime.now().toString()));
//		DBCursor<Lease> cursor = accessor.getLeaseCollection().find(query);
//		if (cursor.size() > 0) {
//			// There are leases with expired deployments. Remove them.
//			do {
//				Lease expiredLease = cursor.next();
//				try {
//					deployer.undeploy(expiredLease.getDeploymentId());
//					// Log the removal
//					pzLogger.log(
//							String.format("Expired Lease with Id %s with expiration date %s for Deployment %s has been removed.",
//									expiredLease.getLeaseId(), expiredLease.getExpiresOn(), expiredLease.getDeploymentId()),
//							Severity.INFORMATIONAL, new AuditElement(ACCESS, "reapExpiredLease", expiredLease.getDeploymentId()));
//				} catch (Exception exception) {
//					String error = String.format("Error reaping Expired Lease with Id %s: %s. This expired lease may still persist.",
//							expiredLease.getLeaseId(), exception.getMessage());
//					LOGGER.error(error, exception);
//					pzLogger.log(error, Severity.ERROR);
//				}
//			} while (cursor.hasNext());
//		} else {
//			// Nothing to do
//			pzLogger.log("There were no expired Deployment Leases to reap.", Severity.INFORMATIONAL);
//		}
	}
}
