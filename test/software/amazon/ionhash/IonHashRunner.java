package software.amazon.ionhash;

import software.amazon.ion.IonContainer;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSexp;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonValue;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.system.IonSystemBuilder;
import software.amazon.ionhash.TestIonHasherProviders.TestIonHasherProvider;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * JUnit Runner that executes IonHashTestSuite.IonHashTest implementations
 * against the test cases defined in ion_hash_tests.ion.
 */
public class IonHashRunner extends Runner {
    private final static IonSystem ION = IonSystemBuilder.standard().build();
    final static String ION_HASH_TESTS_PATH
            = String.format("test/%s/ion_hash_tests.ion", IonHashRunner.class.getPackage().getName().replace('.', '/'));

    private final Class<? extends IonHashTestSuite.IonHashTester> testClass;
    private final IonHashTestSuite.IonHashTester testObject;

    public IonHashRunner(Class<? extends IonHashTestSuite.IonHashTester> testClass) {
        this.testClass = testClass;
        try {
            this.testObject = this.testClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Description getDescription() {
        return Description.createSuiteDescription(testClass);
    }

    @Override
    public void run(RunNotifier notifier) {
        File file = new File(ION_HASH_TESTS_PATH);
        try {
            Iterator<IonValue> iter = ION.iterate(new FileReader(file));
            while (iter.hasNext()) {
                IonStruct test = (IonStruct)iter.next();
                String testName = (test.get("ion") != null)
                        ? test.get("ion").toPrettyString()
                        : "unknown";
                testName = testName.replace("\n", "");
                String[] annotations = test.getTypeAnnotations();
                if (annotations.length > 0) {
                    testName = annotations[0];
                }

                IonStruct expect = (IonStruct)test.get("expect");
                Iterator<IonValue> expectIter = expect.iterator();
                while(expectIter.hasNext()) {
                    IonSexp expectedHashLog = (IonSexp)expectIter.next();
                    String hasherName = expectedHashLog.getFieldName();
                    runTest(hasherName.equals("identity") ? testName : testName + "." + hasherName,
                            test,
                            notifier,
                            expectedHashLog,
                            TestIonHasherProviders.getInstance(hasherName));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void runTest(String testName,
                         IonStruct test,
                         RunNotifier notifier,
                         IonSexp expectedHashLog,
                         TestIonHasherProvider hasherProvider) throws IOException {

        if (expectedHashLog == null) {
            return;
        }
        Description desc = Description.createTestDescription(testClass, testName);
        try {
            notifier.fireTestStarted(desc);

            IonValue ionText = test.get("ion");
            IonValue ionBinary = test.get("10n");
            if (ionText != null && ionBinary != null) {
                throw new Exception("Test must not define both 'ion' and '10n' fields");
            }

            IonReader reader = ionText != null
                    ? testObject.getIonReader(ionText.toPrettyString())
                    : testObject.getIonReader(containerToBytes((IonContainer) ionBinary));

            IonHashReader ihr = new IonHashReaderImpl(reader, hasherProvider);
            testObject.traverse(ihr);
            ihr.close();

            IonSexp actualHashLog = hasherProvider.getHashLog();
            IonSexp actualHashLogFiltered = filterHashLog(actualHashLog, expectedHashLog);
            assertEquals(
                hashLogToString(expectedHashLog),
                hashLogToString(actualHashLogFiltered));
        } catch (AssertionError | Exception e) {
            notifier.fireTestFailure(new Failure(desc, e));
        } finally {
            notifier.fireTestFinished(desc);
        }
    }

    static byte[] containerToBytes(IonContainer container) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // prefix with BVM:
        baos.write((byte)0xE0);
        baos.write(      0x01);
        baos.write(      0x00);
        baos.write((byte)0xEA);
        Iterator<IonValue> bytesIter = container.iterator();
        while (bytesIter.hasNext()) {
            IonInt i = (IonInt)bytesIter.next();
            baos.write((byte)i.intValue());
        }
        return baos.toByteArray();
    }

    // filter out all hash log entries to only those method calls referenced in the expectedHashLog
    private static IonSexp filterHashLog(IonSexp actualHashLog, IonSexp expectedHashLog) {
        Set<String> methodCalls = new HashSet<>();
        Iterator<IonValue> iter = expectedHashLog.iterator();
        while (iter.hasNext()) {
            IonValue v = iter.next();
            for (SymbolToken annotation : v.getTypeAnnotationSymbols()) {
                methodCalls.add(annotation.getText());
            }
        }

        IonSexp result = ION.newSexp();
        if (methodCalls.size() == 1 && methodCalls.contains("final_digest")) {
            IonSexp finalDigest = (IonSexp)actualHashLog.get(actualHashLog.size() - 1).clone();
            finalDigest.setTypeAnnotations("final_digest");
            result.add(finalDigest);
        } else {
            iter = actualHashLog.iterator();
            while (iter.hasNext()) {
                IonValue v = iter.next();
                String methodCall = v.getTypeAnnotations()[0];
                if (methodCalls.contains(methodCall)) {
                    result.add(v.clone());
                }
            }
        }
        return result;
    }

    // enables hex-string display of calls to IonHasher.hash()
    private static String hashLogToString(IonContainer hashLog) {
        boolean multipleEntries = hashLog.size() > 1;
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        Iterator<IonValue> iter = hashLog.iterator();
        while (iter.hasNext()) {
            IonContainer hashCall = (IonContainer)iter.next();
            if (multipleEntries) {
                sb.append("\n  ");
            }
            for (SymbolToken annotation : hashCall.getTypeAnnotationSymbols()) {
                sb.append(annotation.getText()).append("::");
            }
            sb.append("(");
            int cnt = 0;
            Iterator<IonValue> bytesIter = hashCall.iterator();
            while(bytesIter.hasNext()) {
                IonInt i = (IonInt)bytesIter.next();
                if (cnt++ > 0) {
                    sb.append(" ");
                }
                sb.append(String.format("%02x", i.intValue()));
            }
            sb.append(")");
        }
        if (multipleEntries) {
            sb.append("\n");
        }
        sb.append(")");
        return sb.toString();
    }
}
