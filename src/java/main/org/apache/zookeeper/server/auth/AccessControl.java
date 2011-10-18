package org.apache.zookeeper.server.auth;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.InvalidACLException;
import org.apache.zookeeper.common.AccessControlList;
import org.apache.zookeeper.common.AccessControlList.Identifier;
import org.apache.zookeeper.common.AccessControlList.Permission;
import org.apache.zookeeper.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccessControl {
    private static final Logger LOG = LoggerFactory.getLogger(AccessControl.class);

    public final ProviderRegistry providerRegistry = new ProviderRegistry();

    public void check(AccessControlList acl, Permission perm, List<Identifier> ids) throws KeeperException.NoAuthException {
        if(!acl.hasPermission(providerRegistry, perm.toInt(), ids)) throw new KeeperException.NoAuthException();
    }

    /**
     * This method checks out the acl making sure it isn't null or empty,
     * it has valid schemes and ids, and expanding any relative ids that
     * depend on the requestor's authentication information.
     *
     * @param authInfo list of ACL IDs associated with the client connection
     * @param acl list of ACLs being assigned to the node (create or setACL operation)
     */
    public AccessControlList fixup(List<Identifier> authInfo, AccessControlList acl, Path path)
            throws InvalidACLException {
        if (acl == null || acl.isEmpty()) {
            throw new KeeperException.InvalidACLException(path);
        }

        return acl.fixup(providerRegistry, authInfo, path, LOG);
    }

    public static AccessControl create(boolean enabled) {
        if(enabled) {
            return new AccessControl();
        } else {
            LOG.info("zookeeper.skipACL==\"yes\", ACL checks will be skipped");
            return new NoOpAccessControl();
        }
    }
    /**
     * Dummy AccessControl that does nothing. Practically turns off access control.
     */
    public static class NoOpAccessControl extends AccessControl {
        @Override
        public AccessControlList fixup(List<Identifier> authInfo, AccessControlList acl, Path path) {
            return acl;
        }

        @Override
        public void check(AccessControlList acl, Permission perm, List<Identifier> ids) {
        }
    }
}
