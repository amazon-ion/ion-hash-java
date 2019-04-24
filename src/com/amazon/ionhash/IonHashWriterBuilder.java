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
