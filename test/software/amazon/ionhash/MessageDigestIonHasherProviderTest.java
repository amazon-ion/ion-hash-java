package software.amazon.ionhash;

import org.junit.Test;

import static software.amazon.ionhash.TestUtil.assertEquals;

public class MessageDigestIonHasherProviderTest {
    @Test(expected = IonHashException.class)
    public void testInvalidAlgorithm() {
        new MessageDigestIonHasherProvider("invalid algorithm").newHasher();
    }

    @Test
    public void testHasher() {
        // using the flawed MD5 algorithm FOR TEST PURPOSES ONLY
        IonHasherProvider hasherProvider = new MessageDigestIonHasherProvider("MD5");
        IonHasher hasher = hasherProvider.newHasher();
        byte[] emptyHasherDigest = hasher.digest();

        hasher.update(new byte[] {0x0f});
        byte[] digest = hasher.digest();
        byte[] expected = new byte[] {
                (byte)0xd8, 0x38, 0x69, 0x1e, 0x5d, 0x4a, (byte)0xd0, 0x68, 0x79,
                (byte)0xca, 0x72, 0x14, 0x42, (byte)0xe8, (byte)0x83, (byte)0xd4};
        assertEquals(expected, digest);

        // verify that the hasher resets after digest:
        assertEquals(emptyHasherDigest, hasher.digest());
    }
}
