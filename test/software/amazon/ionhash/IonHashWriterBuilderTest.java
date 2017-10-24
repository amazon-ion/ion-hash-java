package software.amazon.ionhash;

import org.junit.Test;
import software.amazon.ion.IonSystem;
import software.amazon.ion.system.IonSystemBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public class IonHashWriterBuilderTest {
    private static IonSystem ION = IonSystemBuilder.standard().build();

    @Test(expected = NullPointerException.class)
    public void testNullIonWriter() throws IOException {
        IonHashWriterBuilder.standard()
                .withHasherProvider(TestIonHasherProviders.getInstance("identity"))
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testNullIonHasher() throws IOException {
        IonHashWriterBuilder.standard()
                .withWriter(ION.newBinaryWriter(new ByteArrayOutputStream()))
                .build();
    }

    @Test
    public void testHappyCase() throws IOException {
        IonHashWriter ihw = IonHashWriterBuilder.standard()
                .withWriter(ION.newBinaryWriter(new ByteArrayOutputStream()))
                .withHasherProvider(TestIonHasherProviders.getInstance("identity"))
                .build();
        assertNotNull(ihw);
    }
}
