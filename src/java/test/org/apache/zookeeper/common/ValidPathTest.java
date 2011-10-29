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

package org.apache.zookeeper.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.apache.zookeeper.KeeperException.InvalidPathException;
import org.apache.zookeeper.ZKTestCase;
import org.junit.Test;

public class ValidPathTest extends ZKTestCase {

    @Test
    public void testInstantiateGoodPaths() {
        ArrayList<String> goodPaths = new ArrayList<String>();
        ArrayList<Exception> failedPaths = new ArrayList<Exception>();

        goodPaths.add("/");
        goodPaths.add("/a");
        goodPaths.add("/1");
        goodPaths.add("/my");
        goodPaths.add("/path");
        goodPaths.add("/another/path");
        goodPaths.add("/another/longer/path");
        goodPaths.add("/a/b/c");
        goodPaths.add("/1/2/3");
        goodPaths.add("/a.");
        goodPaths.add("/.a.");
        goodPaths.add("/.a");
        goodPaths.add("/..a.");
        goodPaths.add("/..a..");
        goodPaths.add("/a..");
        goodPaths.add("/s...");
        goodPaths.add("/...");
        goodPaths.add("/.../three_points");
        goodPaths.add("/-");

        goodPaths.add("/:/a");
        goodPaths.add("/;/a");
        goodPaths.add("/,/a");
        goodPaths.add("/*/a");
        goodPaths.add("/+/a");
        goodPaths.add("/#/a");
        goodPaths.add("/'/a");
        goodPaths.add("/?/a");
        goodPaths.add("/\\/a");
        goodPaths.add("/`/a");
        goodPaths.add("/\u00B4/a"); // ACUTE ACCENT
        goodPaths.add("/^/a");
        goodPaths.add("/\u00B0/a"); // DEGREE SIGN
        goodPaths.add("/!/a");
        goodPaths.add("/\"/a");
        goodPaths.add("/\u00A7/a"); // paragraph sign (SECTION SIGN)
        goodPaths.add("/$/a");
        goodPaths.add("/%/a");
        goodPaths.add("/&/a");
        goodPaths.add("/(/a");
        goodPaths.add("/)/a");
        goodPaths.add("/=/a");
        goodPaths.add("/~/a");
        goodPaths.add("/ /a");

        goodPaths.add("/_/a");
        goodPaths.add("/_/a/A/b/f/h/ee/jh/w/jh/a/4/1/2/3/4/5/6/1/2/3/4/5");
        goodPaths.add("/\u00E4\u00F6\u00FC\u00DF/a"); // small german umlauts
        goodPaths.add("/\u00C4\u00D6\u00DC/a"); // capital german umlauts
        goodPaths.add("/UpPeRCaSe01234567()&%$/a");

        for (String path : goodPaths) {
            try {
                Path pathObj = new ValidPath(path);
                assertTrue(pathObj instanceof ValidPath);
            } catch (InvalidPathException e) {
                failedPaths.add(e);
            }
        }
        if (!failedPaths.isEmpty()) {
            StringBuilder message = new StringBuilder();
            for (Exception e : failedPaths) {
                message.append(e.getMessage());
                message.append(",\n");
            }
            fail(message.toString());
        }
    }

    @Test
    public void testInstantiateBadPathsFails() {
        // Bad path cases are taken accordingly to the zookeeper documentation
        // at
        // http://hadoop.apache.org/zookeeper/docs/current/zookeeperProgrammers.html#ch_zkDataModel
        ArrayList<int[]> badRanges = new ArrayList<int[]>();
        ArrayList<String> badPaths = new ArrayList<String>();
        ArrayList<String> shouldHaveFailed = new ArrayList<String>();

        // The null character (\u0000) cannot be part of a path name.
        badRanges.add(new int[] { 0x0000, 0x0000 });
        // The following characters can't be used because they don't display
        // well, or
        // render in confusing ways: \u0001 - \u0019 and \u007F - \u009F
        // TODO: doc says \u0001 - \u0019 but I believe it should be \u0001 -
        // \u001f
        badRanges.add(new int[] { 0x0001, 0x001F });
        badRanges.add(new int[] { 0x007F, 0x009F });
        // The following characters are not allowed: \ud800 -uF8FFF,
        // \uFFF0-uFFFF,
        // \ uXFFFE - \ uXFFFF (where X is a digit 1 - E), \uF0000 - \uFFFFF.
        badRanges.add(new int[] { 0xd800, 0xF8FFF });
        badRanges.add(new int[] { 0xFFF0, 0xFFFF });
        badRanges.add(new int[] { 0x1FFFE, 0x1FFFF });
        badRanges.add(new int[] { 0x2FFFE, 0x2FFFF });
        badRanges.add(new int[] { 0x3FFFE, 0x3FFFF });
        badRanges.add(new int[] { 0x4FFFE, 0x4FFFF });
        badRanges.add(new int[] { 0x5FFFE, 0x5FFFF });
        badRanges.add(new int[] { 0x6FFFE, 0x6FFFF });
        badRanges.add(new int[] { 0x7FFFE, 0x7FFFF });
        badRanges.add(new int[] { 0x8FFFE, 0x8FFFF });
        badRanges.add(new int[] { 0x9FFFE, 0x9FFFF });
        badRanges.add(new int[] { 0xAFFFE, 0xAFFFF });
        badRanges.add(new int[] { 0xBFFFE, 0xBFFFF });
        badRanges.add(new int[] { 0xCFFFE, 0xCFFFF });
        badRanges.add(new int[] { 0xDFFFE, 0xDFFFF });
        badRanges.add(new int[] { 0xEFFFE, 0xEFFFF });

        badRanges.add(new int[] { 0xF0000, 0xFFFFF });

        for (int[] range : badRanges) {
            for (int i = range[0]; i <= range[1]; ++i) {
                String badChar = new String(new int[] { i }, 0, 1);
                badPaths.add("/test" + badChar);
                badPaths.add("/te" + badChar + "st/end");
            }
        }

        // The "." character can be used as part of another name, but "." and
        // ".."
        // cannot alone be used to indicate a node along a path, because
        // ZooKeeper
        // doesn't use relative paths. The following would be invalid:
        // "/a/b/./c" or
        // "/a/b/../c".

        badPaths.add("/.");
        badPaths.add("/..");
        badPaths.add("/../test");
        badPaths.add("/./test");
        badPaths.add("/test/.");
        badPaths.add("/test/..");
        badPaths.add("/1/./2");
        badPaths.add("/1/../2");

        // The token "zookeeper" is reserved.
        // TODO: should the validation code check for these or is it only
        // convention?
        // badPaths.add("/zookeeper");
        // badPaths.add("/abc/zookeeper");
        // badPaths.add("/abc/zookeeper/cba");

        badPaths.add("//");
        badPaths.add("/a/");
        badPaths.add("/test/");
        badPaths.add(" ");
        badPaths.add("hi");
        badPaths.add("1");
        badPaths.add("");
        badPaths.add("///");
        badPaths.add("//test");
        badPaths.add("/test//");

        for (String path : badPaths) {
            try {
                new ValidPath(path);
                shouldHaveFailed.add(path);
            } catch (InvalidPathException e) {
                // exception is expected
            }
        }
        if (!shouldHaveFailed.isEmpty()) {
            StringBuilder message = new StringBuilder(
                    "These should have failed: ");
            for (String path : shouldHaveFailed) {
                message.append(path);
                message.append(", ");
            }
            fail(message.toString());
        }
    }

    private static ArrayList<String[]> getConcatenationData() {
        ArrayList<String[]> data = new ArrayList<String[]>();
        data.add(new String[] { "/", "/test", "/test" });
        data.add(new String[] { "/test", "/", "/test" });
        data.add(new String[] { "/a", "/b", "/a/b" });
        data.add(new String[] { "/hallo/welt", "/hier", "/hallo/welt/hier" });
        data.add(new String[] { "/", "/", "/" });
        return data;
    }

    private static void assertEqualsPathString(String pathString, ValidPath path) {
        assertEquals(pathString, path.toString());
    }
    @Test
    public void testPrepend() throws InvalidPathException {
        ArrayList<String[]> data = getConcatenationData();

        for (String[] d : data) {
            ValidPath first = new ValidPath(d[0]);
            ValidPath second = new ValidPath(d[1]);
            assertEqualsPathString(d[2], second.prepend(first));
        }
    }

    @Test
    public void testRemoveStart() throws InvalidPathException {
        ArrayList<String[]> data = getConcatenationData();

        for (String[] d : data) {
            ValidPath minuend = new ValidPath(d[2]);
            ValidPath subtrahend = new ValidPath(d[0]);
            try {
                assertEqualsPathString(d[1], minuend.removeBegin(subtrahend));
            } catch (IllegalArgumentException e) {
                fail(d[0] + ", " + d[1] + ", " + d[2] + ": " + e.getMessage());
            }
        }
    }

    @Test
    public void testAppend() throws InvalidPathException {
        ArrayList<String[]> data = getConcatenationData();

        for (String[] d : data) {
            ValidPath first = new ValidPath(d[0]);
            ValidPath second = new ValidPath(d[1]);
            assertEqualsPathString(d[2], first.append(second));
        }
    }

    @Test
    public void testRemoveEnd() throws InvalidPathException {
        ArrayList<String[]> data = getConcatenationData();

        for (String[] d : data) {
            ValidPath minuend = new ValidPath(d[2]);
            ValidPath subtrahend = new ValidPath(d[1]);
            try {
                assertEqualsPathString(d[0], minuend.removeEnd(subtrahend));
            } catch (IllegalArgumentException e) {
                fail(d[0] + ", " + d[1] + ", " + d[2] + ": " + e.getMessage());
            }
        }
    }

    @Test
    public void testBasename() throws InvalidPathException {
        ValidPath testPath;

        testPath = new ValidPath("/");
        assertEquals("/", testPath.basename());

        testPath = new ValidPath("/foo");
        assertEquals("foo", testPath.basename());

        testPath = new ValidPath("/foo/bar/baz");
        assertEquals("baz", testPath.basename());
    }

    @Test
    public void testToString() throws InvalidPathException {
        ArrayList<String[]> data = getConcatenationData();

        for (String[] d : data) {
            for (String path : d) {
                assertEqualsPathString(path, new ValidPath(path));
            }
        }
    }

    @Test
    public void testInstantiateSequentialPath() throws InvalidPathException {
        String[] data = new String[] { "/test/", "/", "/test", "/1" };

        for (String d : data) {
            ValidPath path = new ValidPath(d, true);
            assertEqualsPathString(d, path);
            assertEquals(true, path.isSequential());
        }
    }

    @Test
    public void testInstantiateNonSequentialPath() throws InvalidPathException {
        String[] data = new String[] { "/test", "/", "/test/halo", "/1" };

        for (String d : data) {
            ValidPath path = new ValidPath(d, false);
            assertEqualsPathString(d, path);
            assertEquals(false, path.isSequential());
        }
    }

    @Test
    public void testAppendOnSequentialPathFails() throws InvalidPathException {
        ValidPath eins = new ValidPath("/eins", true);
        ValidPath zwei = new ValidPath("/zwei");
        try {
            eins.append(zwei);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testPrependToSequentialPath() throws InvalidPathException {
        ArrayList<String[]> data = getConcatenationData();

        for (String[] d : data) {
            ValidPath first = new ValidPath(d[0]);
            ValidPath second = new ValidPath(d[1], true);
            assertEqualsPathString(d[2], second.prepend(first));
        }
    }

    @Test
    public void testRemoveStartFromSequentialPath() throws InvalidPathException {
        ArrayList<String[]> data = getConcatenationData();

        for (String[] d : data) {
            ValidPath minuend = new ValidPath(d[2], true);
            ValidPath subtrahend = new ValidPath(d[0]);
            try {
                assertEqualsPathString(d[1], minuend.removeBegin(subtrahend));
            } catch (IllegalArgumentException e) {
                fail(d[0] + ", " + d[1] + ", " + d[2] + ": " + e.getMessage());
            }
        }
    }

	@Test
    public void testRemoveEndFromSequentialPathFails() throws InvalidPathException {
        ValidPath minuend = new ValidPath("/meinpfad/ist/lang/", true);
        try {
            minuend.removeEnd(new ValidPath("/"));
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

	@Test
    public void testRemoveSequentialPathFromAnythingFails() throws InvalidPathException {
        ValidPath minuend = new ValidPath("/meinpfad/ist/lang");
        try {
            minuend.removeEnd(new ValidPath("/", true));
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testGetParent() throws InvalidPathException {
        assertEqualsPathString("/a/b", new ValidPath("/a/b/c").getParent());
        assertEqualsPathString("/a/b", new ValidPath("/a/b/c/", true).getParent());
        assertEqualsPathString("/", new ValidPath("/a", true).getParent());
        assertEqualsPathString("/aa/bb", new ValidPath("/aa/bb/cc").getParent());
        assertEqualsPathString("/aa/bb", new ValidPath("/aa/bb/cc/", true).getParent());
        assertEqualsPathString("/", new ValidPath("/aa", true).getParent());
        assertEquals(null, new ValidPath("/", true).getParent());
    }

    @Test
    public void testGetSequenzeNumber() throws InvalidPathException {
        assertEquals(1234567890, new ValidPath("/1234567890").getSequenceNumber());
        assertEquals(1234567890, new ValidPath("/a/1234567890").getSequenceNumber());
        assertEquals(1234567890, new ValidPath("/a/b1234567890").getSequenceNumber());

        // too short:
        try {
            new ValidPath("/123456789").getSequenceNumber();
            fail("expected Exception");
        } catch (NumberFormatException e) {
            // expected
        }

        // too big:
        try {
            new ValidPath("/9999999999").getSequenceNumber();
            fail("expected Exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // negative:
        try {
            new ValidPath("/-123456789").getSequenceNumber();
            fail("expected Exception");
        } catch (NumberFormatException e) {
            // expected
        }
    }

    @Test
    public void testAppendSequenceNumber() throws InvalidPathException {
        assertEqualsPathString("/abc1234567890",
                new ValidPath("/abc").appendSequenceNumber(1234567890));
        assertEqualsPathString("/abc0001234567",
                new ValidPath("/abc").appendSequenceNumber(1234567));
        assertEqualsPathString("/a/bc1234567890",
                new ValidPath("/a/bc").appendSequenceNumber(1234567890));
        assertEqualsPathString("/a/bc0001234567",
                new ValidPath("/a/bc").appendSequenceNumber(1234567));
        assertEqualsPathString("/a/0001234567",
                new ValidPath("/a/", true).appendSequenceNumber(1234567));
    }

    @Test
    public void testLength() throws InvalidPathException {
        assertEquals(4, new ValidPath("/123").length());
    }

    @Test
    public void testEquality() throws InvalidPathException {
        assertTrue(new ValidPath("/").equals(new ValidPath("/")));
        assertTrue(new ValidPath("/a").equals(new ValidPath("/a")));
        assertTrue(new ValidPath("/a", false).equals(new ValidPath("/a", false)));
        assertTrue(new ValidPath("/a/b").equals(new ValidPath("/a/b")));
        assertTrue(new Path("/").equals(new ValidPath("/")));
        assertTrue(new ValidPath("/").equals(new Path("/")));

        assertFalse(new ValidPath("/a").equals(new ValidPath("/")));
        assertFalse(new ValidPath("/a",true).equals(new ValidPath("/a")));
        assertFalse(new ValidPath("/",true).equals(new ValidPath("/")));
    }

    @Test
    public void testHashCode() throws InvalidPathException {
        assertEquals(new ValidPath("/").hashCode(), new ValidPath("/").hashCode());
        assertEquals(new ValidPath("/a").hashCode(), new ValidPath("/a").hashCode());
        assertEquals(new ValidPath("/a", false).hashCode(), new ValidPath("/a", false).hashCode());
        assertEquals(new ValidPath("/a/b").hashCode(), new ValidPath("/a/b").hashCode());
        assertEquals(new Path("/").hashCode(), new ValidPath("/").hashCode());
        assertEquals(new ValidPath("/").hashCode(), new Path("/").hashCode());
    }

    @Test
    public void testGetHead() throws InvalidPathException {
        assertEquals("", new ValidPath("/").getHead());
        assertEquals("a", new ValidPath("/a").getHead());
        assertEquals("a", new ValidPath("/a/b").getHead());
        assertEquals("aaa", new ValidPath("/aaa/bbb").getHead());
        assertEquals("a", new ValidPath("/a/b/c").getHead());
    }

    @Test
    public void testGetTail() throws InvalidPathException {
        assertEquals(null, new ValidPath("/").getTail());
        assertEquals(null, new ValidPath("/a").getTail());
        assertEquals("/b", new ValidPath("/a/b").getTail().toString());
        assertEquals("/b/c", new ValidPath("/a/b/c").getTail().toString());
        assertEquals("/bbb/ccc", new ValidPath("/aaaa/bbb/ccc").getTail().toString());
    }

}