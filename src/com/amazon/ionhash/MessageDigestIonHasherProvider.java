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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * IonHasherProvider implementation that delegates to java.security.MessageDigest.
 *
 * @see java.security.MessageDigest
 */
public class MessageDigestIonHasherProvider implements IonHasherProvider {
    private final String algorithm;

    public MessageDigestIonHasherProvider(String algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public IonHasher newHasher() {
        try {
            return new IonHasher() {
                private final MessageDigest md = MessageDigest.getInstance(algorithm);

                @Override
                public void update(byte[] bytes) {
                    md.update(bytes);
                }

                @Override
                public byte[] digest() {
                    return md.digest();
                }
            };
        } catch (NoSuchAlgorithmException e) {
            throw new IonHashException(e);
        }
    }
}
