package software.amazon.ionhash;

import java.io.IOException;

/**
 * Lambda-style interface that assists in translating IonReader/IonWriter calls
 * to the appropriate hasher update methods.
 */
interface Updatable {
    void update() throws IOException;
}
