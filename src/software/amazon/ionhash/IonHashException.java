package software.amazon.ionhash;

import software.amazon.ion.IonException;

/**
 * Thrown for unexpected or error conditions while hashing Ion data.
 */
public class IonHashException extends IonException {
    public IonHashException(String message) {
        super(message);
    }

    public IonHashException(String message, Throwable cause) {
        super(message, cause);
    }

    public IonHashException(Throwable cause) {
        super(cause);
    }
}
