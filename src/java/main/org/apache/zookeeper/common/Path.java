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

import java.util.Locale;

import org.apache.zookeeper.KeeperException;

/**
 * An unvalidated path of a znode in the zookeeper data structure.
 *
 * A path can be sequential. This is an exceptional path that is only needed
 * when creating a sequential znode where the znode's basename will only consist
 * of the sequence number given by the server. Then the user needs to construct
 * a path ending in a SEPARATOR, which would otherwise be illegal.
 */
public class Path {
    public static final char SEPARATOR_CHAR = '/';

    public static final String SEPARATOR = new String(
            new char[] { SEPARATOR_CHAR }, 0, 1);

    private static final int SEQUENTIAL_DIGITS = 10;

    protected final String path;

    private boolean sequential = false;

    /**
     * Constructs a valid path object.
     *
     * @param path
     * @throws InvalidPathException
     *             if the path string is not valid.
     */
    public Path(String path) {
        this.path = path;
    }

    /**
     * Constructs a concatenated path from the two given paths.
     *
     * If one of the paths is the root path ("/"), then it's simply ignored.
     *
     * @param firstPath
     *            The first part of the new Path.
     * @param secondPath
     *            The second part of the new Path.
     * @throws IllegalArgumentException
     *             if firstPath is sequential.
     */
    protected Path(Path firstPath, Path secondPath) {
        if (firstPath.isSequential()) {
            throw new IllegalArgumentException(
                    "can not append to sequential path " + firstPath);
        }
        this.sequential = secondPath.isSequential();
        if (firstPath.isRoot()) {
            this.path = secondPath.toString();
        } else if (secondPath.isRoot()) {
            this.path = firstPath.toString();
        } else {
            this.path = firstPath.toString() + secondPath.toString();
        }
    }

    /**
     * Constructs a sequential path if isSequential is true.
     *
     * A sequential path is only needed for a create operation and then only to
     * create a sequential znode without a prefix before the sequence number.
     *
     * @param path
     * @param isSequential
     *            whether the new Path should be sequential
     * @throws InvalidPathException
     */
    public Path(String path, boolean isSequential) {
        this.sequential = isSequential;
        this.path = path;
    }

    protected Path(Path superPath) {
        this.sequential = superPath.sequential;
        this.path = superPath.path;
    }

    /**
     * Whether this Path is the root path ("/").
     *
     */
    public boolean isRoot() {
        return path.equals(SEPARATOR);
    }

    @Override
    public boolean equals(final Object otherObj) {
        if(!(otherObj instanceof Path)) return false;

        Path otherPath = (Path) otherObj;
        if(!otherPath.path.equals(path)) return false;
        if(!otherPath.sequential == sequential) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int sequentialHash = sequential ? 1 : 0;
        return path.hashCode() * 31 + sequentialHash;
    }

    /**
     * Returns the string that was used to construct this Path.
     *
     * It's guaranteed that this String is only the plain path.
     */
    @Override
    public String toString() {
        return path;
    }

    /**
     * Creates a new path starting with the given path and ending with this
     * path.
     *
     * @param path
     *            the first part of the new Path
     * @return a new concatenated Path instance.
     */
    public Path prepend(Path path) {
        return new Path(path, this);
    }

    /**
     * Returns a path instance shortened at the beginning by the given path.
     *
     * TODO check, if path is indeed the first part of this. By now we only cut
     * by length.
     *
     * @param path
     *            the path to be removed from the beginning of this instance
     * @return Path instance
     * @throws IllegalArgumentException
     */
    public Path removeBegin(Path path) {
        if (path.isRoot()) {
            return this;
        } else if (this.isRoot()) {
            return path;
        } else {
            String other = path.toString();

            if (other.length() > this.path.length()) {
                throw new IllegalArgumentException("Can not remove " + other
                        + " from the start of " + this.path);
            }

            String result = this.toString().substring(other.length());

            if(result.length() > 0) {
                return new Path(result);
            } else {
                return new Path(SEPARATOR, this.sequential);
            }
        }
    }

    /**
     * Creates a new path starting with this path and ending with the given
     * path.
     *
     * This operation is illegal if this Path is sequential.
     *
     * @param path
     *            the last part of the new Path
     * @throws IllegalArgumentException
     *             if this path is sequential
     * @return a new concatenated Path instance.
     */
    public Path append(Path path) {
        return new Path(this, path);
    }

    /**
     * Returns the part after the last SEPARATOR of the path.
     *
     * Returns the SEPARATOR, if the path is the root path. Otherwise it works
     * like unix' basename, e.g. basename of "/hello/world" is "world".
     *
     * @return
     */
    public String basename() {
        if (isRoot()) {
            return SEPARATOR;
        }

        return path.substring(path.lastIndexOf(SEPARATOR_CHAR) + 1);
    }

    /**
     * Returns path instance shortened at the end by the given path.
     *
     * This operation is illegal if this Path is sequential.
     *
     * TODO check, if path is indeed the last part of this. By now we only cut
     * by length.
     *
     * @param path
     *            the path to be removed from the end of this instance
     * @return Path instance
     * @throws IllegalArgumentException
     */
    public Path removeEnd(Path path) {
        if (this.sequential) {
            throw new IllegalArgumentException(
                    "can not remove end from sequential path");
        }
        if (path.isSequential()) {
            throw new IllegalArgumentException("can not remove sequential path");
        }

        if (path.isRoot()) {
            return this;
        } else if (this.isRoot()) {
            return path;
        } else {
            String other = path.toString();

            if (other.length() > this.path.length()) {
                throw new IllegalArgumentException("Can not remove " + other
                        + " from the end of " + this.path);
            }

            String result = this.toString().substring(0,
                    this.path.length() - other.length());

            if(result.length() > 0) {
                return new Path(result);
            } else {
                return ValidPath.ROOT;
            }
        }
    }

    /**
     * Whether this is sequential. Only a sequential path may end with a
     * <code>SEPARATOR</code>.
     *
     * The verification of a sequential path is relaxed in this regard, that it
     * accepts a trailing SEPARATOR. But calling <code>append</code> or
     * <code>removeEnd</code> on a sequential path is illegal.
     *
     */
    public boolean isSequential() {
        return sequential;
    }

    /**
     * Get the parent path of this path.
     *
     * The parent path of a path on the first level (like "/here") is the root
     * path. The behavior of this method for the root path can change. You
     * should call isRoot() before using this method!
     */
    public Path getParent() {
        if (isRoot()) {
            return null;
        }
        int index = path.lastIndexOf(SEPARATOR_CHAR);
        if (index == 0) {
            index = 1;

        } else if (index + 1 == path.length()) {
            index = path.lastIndexOf(SEPARATOR_CHAR, index - 1);
        }
        return new Path(path.substring(0, index));
    }

    /**
     * Extracts the sequence number from this path.
     *
     * @throws NumberFormatException, IllegalArgumentException
     *             if there's no valid sequence number
     */
    public int getSequenceNumber() {
        int length = path.length();
        // +1 because we have to count for at least one SEPARATOR
        if (length < SEQUENTIAL_DIGITS + 1) {
            throw new NumberFormatException(
                    "ZNode name is to short to be sequential. Path: " + path);
        }
        int value;
        try {
            value = Integer
                    .parseInt(path.substring(length - SEQUENTIAL_DIGITS));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Can not parse sequence number from Path: " + path, e);
        }

        if (value < 0) {
            throw new NumberFormatException(
                    "Negative sequence numbers don't make sense. Path: " + path);
        }
        return value;
    }

    /**
     * Creates new Path with appended sequence number.
     *
     * TODO: It should be discussed, whether negative sequence numbers should be
     * allowed.
     */
    public Path appendSequenceNumber(int sequenceNumber) {
        return new Path(path + String.format(Locale.ENGLISH, "%010d", sequenceNumber), true);
    }

    /**
     * The character length of this path.
     */
    public int length() {
        return path.length();
    }

    /**
     * Validates this Path and returns a corresponding ValidPath instance if succesful.
     *
     * @throws InvalidPathException
     */
    public ValidPath validate() throws KeeperException.InvalidPathException {
        return new ValidPath(path, sequential);
    }
}
