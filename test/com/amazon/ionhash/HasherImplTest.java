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

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HasherImplTest {
    @Test
    public void escape() {
        // null case
        assertNull(HasherImpl.escape(null));

        // happy cases
        byte[] empty = new byte[] {};
        assertEquals(empty, HasherImpl.escape(empty));

        byte[] bytes = new byte[] {0x10, 0x11, 0x12, 0x13};
        assertEquals(bytes, HasherImpl.escape(bytes));


        // escape cases
        assertArrayEquals(new byte[] {0x0C, 0x0B}, HasherImpl.escape(new byte[] {0x0B}));
        assertArrayEquals(new byte[] {0x0C, 0x0E}, HasherImpl.escape(new byte[] {0x0E}));
        assertArrayEquals(new byte[] {0x0C, 0x0C}, HasherImpl.escape(new byte[] {0x0C}));

        assertArrayEquals(    new byte[] {0x0C, 0x0B, 0x0C, 0x0E, 0x0C, 0x0C},
            HasherImpl.escape(new byte[] {      0x0B,       0x0E,       0x0C}));

        assertArrayEquals(    new byte[] {0x0C, 0x0C, 0x0C, 0x0C},
            HasherImpl.escape(new byte[] {      0x0C,       0x0C}));

        assertArrayEquals(    new byte[] {0x0C, 0x0C, 0x10, 0x0C, 0x0C, 0x11, 0x0C, 0x0C, 0x12, 0x0C, 0x0C},
            HasherImpl.escape(new byte[] {      0x0C, 0x10,       0x0C, 0x11,       0x0C, 0x12,       0x0C}));
    }
}
