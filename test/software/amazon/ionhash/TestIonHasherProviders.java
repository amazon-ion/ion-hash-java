package software.amazon.ionhash;

import software.amazon.ion.IonSexp;
import software.amazon.ion.IonSystem;
import software.amazon.ion.system.IonSystemBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * FOR TEST PURPOSES ONLY!!!
 *
 * Each call to hash() simply returns the input byte[], unmodified.
 * Tracks all hash() calls so IonHashRunner can verify correct behavior
 * of IonhashingReaderImpl.
 *
 * @see IonHashRunner
 */
class TestIonHasherProviders {
    abstract static class TestIonHasherProvider implements IonHasherProvider {
        private final IonSystem ION = IonSystemBuilder.standard().build();
        private final IonSexp hashLog = ION.newEmptySexp();

        void addHashToLog(String method, byte[] hash) {
            IonSexp node = ION.newSexp();
            node.addTypeAnnotation(method);
            for (byte b : hash) {
                node.add(ION.newInt(b & 0xFF));
            }
            hashLog.add(node);
        }

        IonSexp getHashLog() {
            return hashLog;
        }
    }

    static TestIonHasherProvider getInstance(final String algorithm) {
        switch (algorithm) {
            case "identity":
                return new TestIonHasherProvider() {
                    @Override
                    public IonHasher newHasher() {
                        return new IonHasher() {
                            private ByteArrayOutputStream baos = new ByteArrayOutputStream();

                            @Override
                            public void update(byte[] bytes) {
                                addHashToLog("update", bytes);
                                try {
                                    baos.write(bytes);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            @Override
                            public byte[] digest() {
                                byte[] bytes = baos.toByteArray();
                                baos.reset();
                                addHashToLog("digest", bytes);
                                return bytes;
                            }
                        };
                    }
                };

            default:
                return new TestIonHasherProvider() {
                    @Override
                    public IonHasher newHasher() {
                        try {
                            return new IonHasher() {
                                private MessageDigest md = MessageDigest.getInstance(algorithm);

                                @Override
                                public void update(byte[] bytes) {
                                    md.update(bytes);
                                }

                                @Override
                                public byte[] digest() {
                                    byte[] hash = md.digest();
                                    addHashToLog("digest", hash);
                                    return hash;
                                }
                            };
                        } catch (NoSuchAlgorithmException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
        }
    }
}
