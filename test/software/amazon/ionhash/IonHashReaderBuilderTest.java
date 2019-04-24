package software.amazon.ionhash;

import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class IonHashReaderBuilderTest {
    private static IonSystem ION = IonSystemBuilder.standard().build();

    @Test(expected = NullPointerException.class)
    public void testNullIonReader() {
        IonHashReaderBuilder.standard()
                .withHasherProvider(TestIonHasherProviders.getInstance("identity"))
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testNullIonHasher() {
        IonHashReaderBuilder.standard()
                .withReader(ION.newReader(""))
                .build();
    }

    @Test
    public void testHappyCase() {
        IonHashReader ihr = IonHashReaderBuilder.standard()
                .withReader(ION.newReader(""))
                .withHasherProvider(TestIonHasherProviders.getInstance("identity"))
                .build();
        assertNotNull(ihr);
    }
}
