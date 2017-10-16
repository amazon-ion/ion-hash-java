package software.amazon.ionhash;

import software.amazon.ion.IonReader;

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
