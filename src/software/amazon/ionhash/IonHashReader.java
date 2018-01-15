package software.amazon.ionhash;

import software.amazon.ion.IonReader;

/**
 * IonReader extension that provides the hash of the IonValue just nexted
 * past or stepped out of, as defined by the Amazon Ion Hash Specification.
 * <p/>
 * Implementations of this interface are not thread-safe.
 *
 * @see IonReader
 */
public interface IonHashReader extends IonReader {
    /**
     * Provides the hash of the IonValue just nexted past or stepped out of;
     * hashes of partial Ion values are not provided.  If there is no current
     * hash value, returns an empty array.
     * <p/>
     * Implementations must calculate the hash independently of how the Ion
     * is traversed (e.g., the hash of a container must be identical whether
     * the caller skips over it, steps into it, or any combination thereof).
     *
     * @return array of bytes representing the hash of the IonValue just
     * nexted past;  if there is no hash, returns an empty array
     */
    byte[] digest();
}
