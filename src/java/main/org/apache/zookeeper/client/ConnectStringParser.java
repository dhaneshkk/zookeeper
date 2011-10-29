/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.common.ValidPath;

/**
 * A parser for ZooKeeper Client connect strings.
 *
 * This class is not meant to be seen or used outside of ZooKeeper itself.
 *
 * @see org.apache.zookeeper.ZooKeeper
 */
public final class ConnectStringParser {
    private static final int DEFAULT_PORT = 2181;

    private final ValidPath chrootPath;

    private final List<InetSocketAddress> serverAddresses;

    /**
     *
     * @throws IllegalArgumentException
     *             for an invalid chroot path.
     */
    public ConnectStringParser(String connectString) {
        this.chrootPath = parseChroot(connectString);
        this.serverAddresses = parseServerAddresses(connectString);
    }

    public ValidPath getChrootPath() {
        return chrootPath;
    }

    public List<InetSocketAddress> getServerAddresses() {
        return serverAddresses;
    }

    private ValidPath parseChroot(final String connectString) {
        int off = connectString.indexOf('/');
        if (off < 0) {
            return ValidPath.ROOT;
        }

        String chrootPath = connectString.substring(off);
        // ignore "/" chroot spec, same as null
        if (chrootPath.length() == 1) {
            return ValidPath.ROOT;
        } else {
            return ValidPath.createUnchecked(chrootPath);
        }
    }

    private List<InetSocketAddress> parseServerAddresses(final String connectString) {
        String hostsList[];
        if(connectString.indexOf('/')>=0) {
            hostsList = connectString.substring(0, connectString.indexOf('/')).split(",");
        } else {
            hostsList = connectString.split(",");
        }

        List<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>(hostsList.length);
        for (String host : hostsList) {
            int port = DEFAULT_PORT;
            int pidx = host.lastIndexOf(':');
            if (pidx >= 0) {
                // otherwise : is at the end of the string, ignore
                if (pidx < host.length() - 1) {
                    port = Integer.parseInt(host.substring(pidx + 1));
                }
                host = host.substring(0, pidx);
            }
            serverAddresses.add(InetSocketAddress.createUnresolved(host, port));
        }
        return serverAddresses;
    }
}