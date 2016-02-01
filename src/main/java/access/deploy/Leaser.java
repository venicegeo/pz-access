package access.deploy;

import org.springframework.stereotype.Component;

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

}
