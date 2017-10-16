package software.amazon.ionhash;

/**
 * An implementation of this interface provides IonHasher instances to an Ion hash
 * implementation as needed.
 * <p/>
 * Implementations must be thread-safe.
 */
public interface IonHasherProvider {
    /**
     * Returns a new IonHasher instance.
     */
    IonHasher newHasher();
}
