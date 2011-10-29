package org.apache.zookeeper.common;

import static org.junit.Assert.assertEquals;

import org.apache.zookeeper.ZooDefs.OpCode;
import org.junit.Test;

public class OpCodeTest {

    @Test
    public void testGetForInt() {
        int[] codeInts = new int[]{0,1,2,3,4,5,6,7,8,9,11,12,13,14,100,101,102,-10,-11,-1};
        for(int codeInt : codeInts) {
            OpCode opCode = OpCode.fromInt(codeInt);
            assertEquals(codeInt, opCode.getInt());
        }
    }
}
