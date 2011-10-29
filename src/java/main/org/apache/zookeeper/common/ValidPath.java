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

import org.apache.zookeeper.KeeperException;

/**
 * A valid path of a znode in the zookeeper data structure.
 *
 * It is guaranteed, that an instance of this class always represents a valid
 * path. Therefore you do not need to run any validation on this path anymore.
 *
 * A path can be sequential. This is an exceptional path that is only needed
 * when creating a sequential znode where the znode's basename will only consist
 * of the sequence number given by the server. Then the user needs to construct
 * a path ending in a SEPARATOR, which would otherwise be illegal.
 */
public final class ValidPath extends Path {

    public static final ValidPath ROOT = ValidPath.createUnchecked(SEPARATOR);

    /**
     * Constructs a valid path object.
     *
     * @param path
     * @throws InvalidPathException
     *             if the path string is not valid.
     */
    public ValidPath(String path) throws KeeperException.InvalidPathException {
        super(path);
        validatePath(path);
    }

    /**
     * Ugly constructor only used if you're absolute sure the path is valid.
     *
     * This constructor does not call the validate method!
     *
     * @param iKnowThisPathIsValid
     *            only used to distinct this Constructor from the default one
     * @param path
     *            a valid(!) path string
     */
    private ValidPath(boolean iKnowThisPathIsValid, String path) {
        super(path);
        if(!iKnowThisPathIsValid) {
            throw new IllegalArgumentException("You shouldn't call this if you're not sure!");
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
    public ValidPath(String path, boolean isSequential) throws KeeperException.InvalidPathException {
        super(path, isSequential);
        if(isSequential) {
            validatePath(path + "1");
        } else {
            validatePath(path);
        }
    }

    private ValidPath(ValidPath firstPath, ValidPath secondPath) {
        super(firstPath, secondPath);
    }

    private ValidPath(Path superPath) {
        super(superPath);
    }

    public String getHead() {
        int index = path.indexOf(SEPARATOR_CHAR, 1);
        if(-1 == index) {
            return path.substring(1);
        } else {
            return path.substring(1, index);
        }
    }

    public ValidPath getTail() {
        int index = path.indexOf(SEPARATOR_CHAR, 1);
        if(-1 == index) {
            return null;
        } else {
            return new ValidPath(new Path(path.substring(index)));
        }
    }

    public ValidPath append(ValidPath path) {
        return new ValidPath(this, path);
    }

    public ValidPath prepend(ValidPath path) {
        return new ValidPath(path, this);
    }

    public ValidPath removeBegin(ValidPath path) {
        return new ValidPath(super.removeBegin(path));
    }

    public ValidPath removeEnd(ValidPath path) {
        return new ValidPath(super.removeEnd(path));
    }

    public ValidPath getParent() {
        Path superParent = super.getParent();
        if(superParent==null) return null;
        return new ValidPath(super.getParent());
    }

    public ValidPath appendSequenceNumber(int sequenceNumber) {
        return new ValidPath(super.appendSequenceNumber(sequenceNumber));
    }

    /**
     * Verifies, whether path represents a valid full path to a znode.
     *
     * You may rather create a Path object and work with this instead of calling
     * this method.
     *
     * @param path
     * @throws InvalidPathException
     *             if the path is not valid.
     */
    public static void validatePath(String path) throws KeeperException.InvalidPathException {
        if (path == null) {
            throw new KeeperException.InvalidPathException("Path cannot be null");
        }
        if (path.length() == 0) {
            throw new KeeperException.InvalidPathException("Path length must be > 0");
        }
        if (path.charAt(0) != SEPARATOR_CHAR) {
            throw new KeeperException.InvalidPathException("Path must start with " + SEPARATOR_CHAR
                    + " character, got: <" + path + ">");
        }
        if (path.length() == 1) { // done checking - it's the root
            return;
        }
        if (path.charAt(path.length() - 1) == SEPARATOR_CHAR) {
            throw new KeeperException.InvalidPathException("Path must not end with "
                    + SEPARATOR_CHAR + " character, got: <" + path + ">");
        }

        String reason = null;
        char lastc = SEPARATOR_CHAR;
        char chars[] = path.toCharArray();
        char c;
        for (int i = 1; i < chars.length; lastc = chars[i], i++) {
            c = chars[i];

            if (c == 0) {
                reason = "null character not allowed @" + i;
                break;
            } else if (c == SEPARATOR_CHAR && lastc == SEPARATOR_CHAR) {
                reason = "empty node name specified @" + i;
                break;
            } else if (c == '.' && lastc == '.') {
                if (chars[i - 2] == SEPARATOR_CHAR
                        && ((i + 1 == chars.length) || chars[i + 1] == SEPARATOR_CHAR)) {
                    reason = "relative paths not allowed @" + i;
                    break;
                }
            } else if (c == '.') {
                if (chars[i - 1] == SEPARATOR_CHAR
                        && ((i + 1 == chars.length) || chars[i + 1] == SEPARATOR_CHAR)) {
                    reason = "relative paths not allowed @" + i;
                    break;
                }
            } else if (c > '\u0000' && c <= '\u001f' || c >= '\u007f'
                    && c <= '\u009F' || c >= '\ud800') {
                // TODO: The forbidden characters in the ZooKeeper documentation
                // and the original validation code are not in sync. I'm
                // conservative here.
                reason = "invalid charater @" + i;
                break;
            }
        }

        if (reason != null) {
            throw new KeeperException.InvalidPathException("Invalid path string \"" + path
                    + "\" caused by " + reason);
        }
    }

    /**
     * Ctor that throws an unchecked exception instead of InvalidPathException.
     *
     */
    public static ValidPath createUnchecked(String pathString, boolean isSequential) {
        try {
            return new ValidPath(pathString, isSequential);
        } catch (KeeperException.InvalidPathException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Ctor that throws an unchecked exception instead of InvalidPathException.
     *
     */
    public static ValidPath createUnchecked(String pathString) {
        try {
            return new ValidPath(pathString);
        } catch (KeeperException.InvalidPathException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
