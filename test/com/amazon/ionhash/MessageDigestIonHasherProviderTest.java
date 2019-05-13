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

import static com.amazon.ionhash.TestUtil.assertEquals;

public class MessageDigestIonHasherProviderTest {
    @Test(expected = IonHashException.class)
    public void testInvalidAlgorithm() {
        new MessageDigestIonHasherProvider("invalid algorithm").newHasher();
    }

    @Test
    public void testHasher() {
        // using the flawed MD5 algorithm FOR TEST PURPOSES ONLY
        IonHasherProvider hasherProvider = new MessageDigestIonHasherProvider("MD5");
        IonHasher hasher = hasherProvider.newHasher();
        byte[] emptyHasherDigest = hasher.digest();

        hasher.update(new byte[] {0x0f});
        byte[] digest = hasher.digest();
        byte[] expected = new byte[] {
                (byte)0xd8, 0x38, 0x69, 0x1e, 0x5d, 0x4a, (byte)0xd0, 0x68, 0x79,
                (byte)0xca, 0x72, 0x14, 0x42, (byte)0xe8, (byte)0x83, (byte)0xd4};
        assertEquals(expected, digest);

        // verify that the hasher resets after digest:
        assertEquals(emptyHasherDigest, hasher.digest());
    }
}
