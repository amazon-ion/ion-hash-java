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

import com.amazon.ion.IonWriter;

import java.io.IOException;

/**
 * Build a new {@link IonHashWriter} for the given {@link IonWriter} and {@link IonHasherProvider}.
 * <p/>
 * Instances of this class are not thread-safe.
 */
public class IonHashWriterBuilder {
    private IonWriter writer;
    private IonHasherProvider hasherProvider;

    /**
     * The standard builder of {@link IonHashWriterBuilder}s.
     */
    public static IonHashWriterBuilder standard() {
        return new IonHashWriterBuilder();
    }

    // no public constructor
    private IonHashWriterBuilder() {
    }

    /**
     * Specifies the stream writer to compute hashes over.
     */
    public IonHashWriterBuilder withWriter(IonWriter writer) {
        this.writer = writer;
        return this;
    }

    /**
     * Specifies the hash function implementation to use.
     */
    public IonHashWriterBuilder withHasherProvider(IonHasherProvider hasherProvider) {
        this.hasherProvider = hasherProvider;
        return this;
    }

    /**
     * Constructs a new IonHashWriter, which decorates the IonWriter with hashes.
     *
     * @return a new IonHashWriter object
     */
    public IonHashWriter build() throws IOException {
        return new IonHashWriterImpl(writer, hasherProvider);
    }
}
