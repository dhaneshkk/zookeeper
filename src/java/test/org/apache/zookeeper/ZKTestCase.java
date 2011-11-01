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

package org.apache.zookeeper;

import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.server.Transaction;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.Rule;
import org.junit.rules.MethodRule;
import org.junit.rules.TestWatchman;
import org.junit.runner.RunWith;
import org.junit.runners.model.FrameworkMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for a non-parameterized ZK test.
 *
 * Basic utilities shared by all tests. Also logging of various events during
 * the test execution (start/stop/success/failure/etc...)
 */
@RunWith(JUnit4ZKTestRunner.class)
public class ZKTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(ZKTestCase.class);

    private String testName;

    protected String getTestName() {
        return testName;
    }

    @Rule
    public MethodRule watchman = new TestWatchman() {
        @Override
        public void starting(FrameworkMethod method) {
            testName = method.getName();
            LOG.info("STARTING " + testName);
        }

        @Override
        public void finished(FrameworkMethod method) {
            LOG.info("FINISHED " + testName);
        }

        @Override
        public void succeeded(FrameworkMethod method) {
            LOG.info("SUCCEEDED " + testName);
        }

        @Override
        public void failed(Throwable e, FrameworkMethod method) {
            LOG.info("FAILED " + testName, e);
        }

    };

    protected static Transaction.Create buildCreateTransaction(final String path, byte data[], List<ACL> acl,
            long ephemeralOwner, int parentCVersion, long zxid, long time)
            throws KeeperException.NoNodeException,
            KeeperException.NodeExistsException {
        TxnHeader hdr = new TxnHeader();
        hdr.setClientId(ephemeralOwner);
        hdr.setZxid(zxid);
        hdr.setTime(time);

        CreateTxn txn = new CreateTxn(path, data, acl, true, parentCVersion);
        return new Transaction.Create(hdr, txn);
    }
}
