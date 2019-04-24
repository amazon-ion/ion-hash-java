package com.amazon.ionhash;

import com.amazon.ion.IonWriter;

/**
 * IonWriter extension that provides the hash of the IonValue just written
 * or stepped out of, as defined by the Amazon Ion Hash Specification.
 * <p/>
 * Implementations of this interface are not thread-safe.
 *
 * @see IonWriter
 */
public interface IonHashWriter extends IonWriter {
    /**
     * Provides the hash of the IonValue just nexted past or stepped out of;
     * hashes of partial Ion values are not provided.  If there is no current
     * hash value, returns an empty array.
     *
     * @return array of bytes representing the hash of the IonValue just
     * written or stepped out of;  if there is no hash, returns an empty array
     */
    byte[] digest();
}
