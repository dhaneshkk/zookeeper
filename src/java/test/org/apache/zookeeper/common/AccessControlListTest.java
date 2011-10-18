package org.apache.zookeeper.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.zookeeper.common.AccessControlList.Permission;
import org.junit.Test;

public class AccessControlListTest {
    private static String randomSuffix = String.valueOf(Math.random());
    private static String id0 = "id" + randomSuffix;
    private static String id1 = "id" + randomSuffix;

    @Test
    public void testIdStringsAreEqualButDifferentObjects() {
        assertTrue(id0.equals(id1));
        assertTrue(id1.equals(id0));
        assertFalse(id0==id1);
    }

    @Test
    public void testEqualsOnEmptyACL() {
        AccessControlList acl = (new AccessControlList.Builder()).build();
        assertTrue(acl.equals(AccessControlList.EMPTY));
        assertTrue(AccessControlList.EMPTY.equals(acl));
        assertEquals(acl.hashCode(), AccessControlList.EMPTY.hashCode());
    }

    @Test
    public void testNotEqualsWithEmpty() {
        AccessControlList acl = (new AccessControlList.Builder())
                .add(Permission.CREATE.toInt(), "openid", "me").build();
        assertFalse(acl.equals(AccessControlList.EMPTY));
        assertFalse(AccessControlList.EMPTY.equals(acl));
    }

    @Test
    public void testEqualsWithNonEmptyACL() {

        AccessControlList acl = (new AccessControlList.Builder())
                .add(Permission.CREATE.toInt(), "openid", id0).build();
        AccessControlList otherAcl = (new AccessControlList.Builder())
                .add(Permission.CREATE.toInt(), "openid", id1).build();
        assertTrue(acl.equals(otherAcl));
        assertTrue(otherAcl.equals(acl));
        assertEquals(acl.hashCode(), otherAcl.hashCode());
    }

    @Test
    public void testEqualsWithTwoEntries() {
        // Add the entries in different order. This should still result in equal ACLs
        AccessControlList acl = (new AccessControlList.Builder())
                .add(Permission.CREATE.toInt(), "kerberos", id0)
                .add(Permission.CREATE.toInt(), "openid", id1).build();
        AccessControlList otherAcl = (new AccessControlList.Builder())
                .add(Permission.CREATE.toInt(), "openid", id1)
                .add(Permission.CREATE.toInt(), "kerberos", id0).build();
        assertTrue(acl.equals(otherAcl));
        assertTrue(otherAcl.equals(acl));
        assertEquals(acl.hashCode(), otherAcl.hashCode());
    }

    @Test
    public void testIgnoresDoubleInserts() {
        AccessControlList acl = (new AccessControlList.Builder())
                .add(Permission.CREATE.toInt(), "openid", id1)
                .add(Permission.CREATE.toInt(), "openid", id0).build();
        AccessControlList otherAcl = (new AccessControlList.Builder())
                .add(Permission.CREATE.toInt(), "openid", id0).build();
        assertTrue(acl.equals(otherAcl));
        assertTrue(otherAcl.equals(acl));
        assertEquals(acl.hashCode(), otherAcl.hashCode());
    }
}
