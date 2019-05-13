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

/**
 * User-provided hash function that is required by the Amazon Ion Hashing
 * Specification.
 * <p/>
 * Implementations are not required to be thread-safe.
 */
public interface IonHasher {
    /**
     * Updates the hash with the specified array of bytes.
     *
     * @param bytes the bytes to hash
     */
    void update(byte[] bytes);

    /**
     * Returns the computed hash bytes and resets any internal state
     * so the hasher may be reused.
     */
    byte[] digest();
}
