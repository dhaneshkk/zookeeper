package org.apache.zookeeper.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.InvalidACLException;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.slf4j.Logger;

public class AccessControlList {
    public static final String WORLD = "world";
    public static final String ANYONE = "anyone";
    public static final String AUTH = "auth";
    public static final String SUPER = "super";

    public static final AccessControlList EMPTY = new AccessControlList(new TreeSet<Entry>());
    public static final AccessControlList OPEN_ACL_UNSAFE = new AccessControlList(
            new TreeSet<Entry>(Collections.singletonList(new Entry(Perms.ALL, "world", "anyone"))));
    private final SortedSet<Entry> entries;

    public AccessControlList(final SortedSet<Entry> entries) {
        // copy the list to make this immutable and have the entries Set shrinked
        // to the size of the list. Remove duplicates.
        this.entries = new TreeSet<Entry>();
        outer: for(Entry newEntry : entries) {
            for(Entry alreadyAddedEntry : this.entries) {
                if(alreadyAddedEntry.equals(newEntry)) continue outer;
            }
            this.entries.add(newEntry);
        }
    }

    public boolean hasPermission(ProviderRegistry providerRegistry, int perm, List<Identifier> ids) throws KeeperException.NoAuthException {
        if (entries.isEmpty()) {
            return true;
        }
        for (Identifier authId : ids) {
            if (authId.getScheme().equals(SUPER)) {
                return true;
            }
        }
        for (Entry entry : entries) {
            if ((entry.perms & perm) != 0) {
                if (entry.matchesAnybody()) {
                    return true;
                }
                // maybe call this method with a Map of available Providers?
                AuthenticationProvider ap = providerRegistry.getProvider(entry.scheme);
                if (ap != null) {
                    for (Identifier authId : ids) {
                        if(entry.matches(authId, ap)) return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /**
     *
     * @param authInfo
     * @param path
     * @param log
     * @return
     * @throws InvalidACLException
     */
    public AccessControlList fixup(ProviderRegistry providerRegistry, List<Identifier> authInfo, Path path, Logger log) throws InvalidACLException {
        Builder builder = new Builder();
        for (Entry entry : entries) {
            if (entry.matchesAnybody()) {
                builder.add(entry.perms, entry.scheme, entry.id);
            } else if (entry.scheme.equals(AUTH)) {
                // This is the "auth" id, so we have to expand it to the
                // authenticated ids of the requestor

                boolean authIdValid = false;
                for (Identifier cid : authInfo) {
                    AuthenticationProvider ap = providerRegistry.getProvider(cid.getScheme());
                    if (ap == null) {
                        log.error("Missing AuthenticationProvider for " + cid.getScheme());
                    } else if (ap.isAuthenticated()) {
                        authIdValid = true;
                        builder.add(entry.perms, cid.getScheme(), cid.getId());
                    }
                }
                if (!authIdValid) {
                    throw new KeeperException.InvalidACLException(path);
                }
            } else {
                AuthenticationProvider ap = providerRegistry.getProvider(entry.scheme);
                if (ap == null) {
                    throw new KeeperException.InvalidACLException(path);
                }
                if (!ap.isValid(entry.id)) {
                    throw new KeeperException.InvalidACLException(path);
                }
                builder.add(entry.perms, entry.scheme, entry.id);
            }
        }

        AccessControlList newAcl = builder.build();
        if (newAcl.isEmpty())
            throw new KeeperException.InvalidACLException(path);
        return newAcl;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof AccessControlList)) return false;

        AccessControlList acl = (AccessControlList) o;
        Iterator<Entry> it0 = this.entries.iterator();
        Iterator<Entry> it1 = acl.entries.iterator();

        while(it0.hasNext() && it1.hasNext()) {
            if(!it0.next().equals(it1.next())) return false;
        }

        // make sure that we exited the loop because _both_ iterators finished
        return it0.hasNext() == it1.hasNext();
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    public List<ACL> toJuteACL() {
        List<ACL> acl = new ArrayList<ACL>(entries.size());
        for(Entry entry : entries) {
            acl.add(new ACL(entry.perms, new Id(entry.scheme, entry.id)));
        }
        return acl;
    }

    public static AccessControlList fromJuteACL(List<ACL> acl) {
        if(acl == null) return AccessControlList.EMPTY;

        Builder builder = new Builder();
        for(ACL a : acl) {
            builder.add(a.getPerms(), a.getId().getScheme(), a.getId().getId());
        }
        return builder.build();
    }

    public static class Builder {
        private final SortedSet<Entry> entries = new TreeSet<Entry>();

        public Builder add(int perms, String scheme, String id) {
            entries.add(new Entry(perms, scheme, id));
            return this;
        }

        public AccessControlList build() {
            if(entries.isEmpty()) return AccessControlList.EMPTY;
            return new AccessControlList(entries);
        }
    }

    public static final class Identifier {
        private final String scheme;
        private final String id;

        public Identifier(String scheme, String id) {
            this.scheme = scheme;
            this.id = id;
        }

        public String getScheme() {
            return scheme;
        }

        public String getId() {
            return id;
        }

        public static List<Id> toJuteIdList(Collection<Identifier> identifiers) {
            if(identifiers == null) return null;
            List<Id> juteIdList = new ArrayList<Id>(identifiers.size());
            for(Identifier id : identifiers) {
                juteIdList.add(new Id(id.getScheme(), id.getId()));
            }
            return juteIdList;
        }

        public static List<Identifier> fromJuteIdList(List<Id> juteIdList) {
            if(juteIdList == null) return null;
            List<Identifier> identifiers = new ArrayList<Identifier>(juteIdList.size());
            for(Id id : juteIdList) {
                identifiers.add(new Identifier(id.getScheme(), id.getId()));
            }
            return identifiers;
        }
    }

    public static enum Permission {
        READ(1 << 0),
        WRITE(1 << 1),
        CREATE(1 << 2),
        DELETE(1 << 3),
        ADMIN(1 << 4),
        ALL(READ.bitmask | WRITE.bitmask | CREATE.bitmask | DELETE.bitmask | ADMIN.bitmask);

        private int bitmask;

        Permission(int bitmask) {
            this.bitmask = bitmask;
        }

        public int toInt() {
            return bitmask;
        }
    }

    private static class Entry implements Comparable<Entry>{
        private final int perms;
        private final String scheme;
        private final String id;

        public Entry(int perms, String scheme, String id) {
            if(scheme == null) throw new IllegalArgumentException("scheme may not be null");
            if(id == null) throw new IllegalArgumentException("id may not be null");
            this.perms=perms;
            this.scheme=scheme;
            this.id=id;
        }

        public boolean matchesAnybody() {
            return WORLD.equals(scheme) && ANYONE.equals(id);
        }

        public boolean matches(Identifier authId, AuthenticationProvider ap) {
            return authId.getScheme().equals(scheme)
                && ap.matches(authId.getId(), id);
        }

        @Override
        public int compareTo(Entry o) {
            int comparission;
            comparission = scheme.compareTo(o.scheme);
            if(comparission != 0) return comparission;

            comparission = id.compareTo(o.id);
            if(comparission != 0) return comparission;

            if(perms < o.perms) return -1;
            if(perms > o.perms) return 1;

            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Entry other = (Entry) obj;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (perms != other.perms)
                return false;
            if (scheme == null) {
                if (other.scheme != null)
                    return false;
            } else if (!scheme.equals(other.scheme))
                return false;
            return true;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + perms;
            result = prime * result + ((scheme == null) ? 0 : scheme.hashCode());
            return result;
        }
    }
}
