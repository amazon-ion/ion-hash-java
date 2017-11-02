package software.amazon.ionhash;

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
