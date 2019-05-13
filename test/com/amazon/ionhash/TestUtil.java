/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.ionhash;

import org.junit.Assert;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;

/**
 * Test utility methods.
 */
class TestUtil {
    private static final IonSystem ION = IonSystemBuilder.standard().build();

    static void assertEquals(byte[] expected, byte[] actual) {
        assertEquals(null, expected, actual);
    }

    static void assertEquals(String message, byte[] expected, byte[] actual) {
        Assert.assertEquals(message, bytesToHex(expected), bytesToHex(actual));
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b));
        }
        return sb.toString();
    }

    static byte[] sexpToBytes(String sexpString) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IonSexp sexp = (IonSexp)ION.singleValue(sexpString);
        Iterator<IonValue> iter = sexp.iterator();
        while (iter.hasNext()) {
            IonInt i = (IonInt)iter.next();
            baos.write(i.intValue());
        }
        return baos.toByteArray();
    }
}
