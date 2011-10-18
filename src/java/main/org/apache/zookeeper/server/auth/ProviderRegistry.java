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

package org.apache.zookeeper.server.auth;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.server.ZooKeeperServer;

public final class ProviderRegistry {
    public static final String PROPERTIES_PREFIX = "zookeeper.authProvider.";

    private static final Logger LOG = LoggerFactory.getLogger(ProviderRegistry.class);

    private final Map<String, AuthenticationProvider> authenticationProviders =
        new HashMap<String, AuthenticationProvider>();

    public ProviderRegistry() {
        register(new IPAuthenticationProvider());
        register(new DigestAuthenticationProvider());

        ClassLoader classLoader = ZooKeeperServer.class.getClassLoader();
        for(String key : new PropertyKeyIterator()) {
            String className = System.getProperty(key);
            try {
                Class<?> c = classLoader.loadClass(className);
                register((AuthenticationProvider) c.newInstance());

            } catch (Exception e) {
                LOG.warn("Problems loading " + className, e);
            }
        }
    }

    private void register(AuthenticationProvider ap) {
        authenticationProviders.put(ap.getScheme(), ap);
    }

    public AuthenticationProvider getProvider(String scheme) {
        return authenticationProviders.get(scheme);
    }

    public String listProviders() {
        StringBuilder sb = new StringBuilder();
        for(String s: authenticationProviders.keySet()) {
            sb.append(s + " ");
        }
        return sb.toString();
    }

    private static class PropertyKeyIterator implements Iterator<String>, Iterable<String> {
        private final Enumeration<Object> en = System.getProperties().keys();
        private String next;

        @Override
        public boolean hasNext() {
            while(en.hasMoreElements()) {
                String key = (String) en.nextElement();
                if(key.startsWith(PROPERTIES_PREFIX)) {
                    next = key;
                    return true;
                }
            }
            return false;
        }

        @Override
        public String next() {
            return next;
        }

        @Override
        public void remove() {}

        @Override
        public Iterator<String> iterator() {
            return this;
        }
    }
}
