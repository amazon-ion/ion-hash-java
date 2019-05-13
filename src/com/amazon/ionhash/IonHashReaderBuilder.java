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

import com.amazon.ion.IonReader;

/**
 * Build a new {@link IonHashReader} for the given {@link IonReader} and {@link IonHasherProvider}.
 * <p/>
 * Instances of this class are not thread-safe.
 */
public class IonHashReaderBuilder {
    private IonReader reader;
    private IonHasherProvider hasherProvider;

    /**
     * The standard builder of {@link IonHashReaderBuilder}s.
     */
    public static IonHashReaderBuilder standard() {
        return new IonHashReaderBuilder();
    }

    // no public constructor
    private IonHashReaderBuilder() {
    }

    /**
     * Specifies the stream reader to compute hashes over.
     */
    public IonHashReaderBuilder withReader(IonReader reader) {
        this.reader = reader;
        return this;
    }

    /**
     * Specifies the hash function implementation to use.
     */
    public IonHashReaderBuilder withHasherProvider(IonHasherProvider hasherProvider) {
        this.hasherProvider = hasherProvider;
        return this;
    }

    /**
     * Constructs a new IonHashReader, which decorates the IonReader with hashes.
     *
     * @return a new IonHashReader object
     */
    public IonHashReader build() {
        return new IonHashReaderImpl(reader, hasherProvider);
    }
}
