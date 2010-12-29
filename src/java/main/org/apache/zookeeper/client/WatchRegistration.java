package org.apache.zookeeper.client;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

/**
 * Register a watcher for a particular path.
 * 
 * This class and its subclasses are not part of the public ZooKeeper API!
 */
public abstract class WatchRegistration {
    private Watcher watcher;
    private String clientPath;
    public WatchRegistration(Watcher watcher, String clientPath)
    {
        this.watcher = watcher;
        this.clientPath = clientPath;
    }

    abstract protected Map<String, Set<Watcher>> getWatches(WatchManager watchManager, int rc);

    /**
     * Register the watcher with the set of watches on path.
     * @param rc the result code of the operation that attempted to
     * add the watch on the path.
     */
    public void register(WatchManager watchManager, int rc) {
        if (shouldAddWatch(rc)) {
            Map<String, Set<Watcher>> watches = getWatches(watchManager, rc);
            synchronized(watches) {
                Set<Watcher> watchers = watches.get(clientPath);
                if (watchers == null) {
                    watchers = new HashSet<Watcher>();
                    watches.put(clientPath, watchers);
                }
                watchers.add(watcher);
            }
        }
    }
    /**
     * Determine whether the watch should be added based on return code.
     * @param rc the result code of the operation that attempted to add the
     * watch on the node
     * @return true if the watch should be added, otw false
     */
    protected boolean shouldAddWatch(int rc) {
        return rc == 0;
    }
    
    /** Handle the special case of exists watches - they add a watcher
     * even in the case where NONODE result code is returned.
     */
    public static class Exists extends WatchRegistration {
        public Exists(Watcher watcher, String clientPath) {
            super(watcher, clientPath);
        }

        @Override
        protected Map<String, Set<Watcher>> getWatches(WatchManager watchManager, int rc) {
            return rc == 0 ?  watchManager.dataWatches : watchManager.existWatches;
        }

        @Override
        protected boolean shouldAddWatch(int rc) {
            return rc == 0 || rc == KeeperException.Code.NONODE.intValue();
        }
    }

    public static class Data extends WatchRegistration {
        public Data(Watcher watcher, String clientPath) {
            super(watcher, clientPath);
        }

        @Override
        protected Map<String, Set<Watcher>> getWatches(WatchManager watchManager, int rc) {
            return watchManager.dataWatches;
        }
    }

    public static class Child extends WatchRegistration {
        public Child(Watcher watcher, String clientPath) {
            super(watcher, clientPath);
        }

        @Override
        protected Map<String, Set<Watcher>> getWatches(WatchManager watchManager, int rc) {
            return watchManager.childWatches;
        }
    }
}