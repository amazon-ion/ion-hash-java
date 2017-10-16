package software.amazon.ionhash;

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
