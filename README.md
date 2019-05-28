# Amazon Ion Hash Java
An implementation of [Amazon Ion Hash](http://amzn.github.io/ion-hash) in Java.

[![Build Status](https://travis-ci.org/amzn/ion-hash-java.svg?branch=master)](https://travis-ci.org/amzn/ion-hash-java)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.amazon.ion/ion-hash-java/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.amazon.ion/ion-hash-java)
[![Javadoc](https://javadoc-badge.appspot.com/com.amazon.ion/ion-hash-java.svg?label=javadoc)](http://www.javadoc.io/doc/com.amazon.ion/ion-hash-java)

## Depending on the Library
To start using `ion-hash-java` in your code with Maven, insert the following
dependency into your project's `pom.xml`, replacing ${version} with the
desired version of the library (e.g., 1.0.0):

```xml
<dependency>
  <groupId>com.amazon.ion</groupId>
  <artifactId>ion-hash-java</artifactId>
  <version>${version}</version>
</dependency>
```

## Example Usage
The following example computes a hash while writing a simple Ion struct,
then computes the same hash while reading the written value.  This illustrates
a typical pattern of writing data, computing its hash, then at some subsequent
tiem, verifying the hash matches when the data is read.

```java
package ionhash.example;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ionhash.IonHashReader;
import com.amazon.ionhash.IonHashReaderBuilder;
import com.amazon.ionhash.IonHashWriter;
import com.amazon.ionhash.IonHashWriterBuilder;
import com.amazon.ionhash.IonHasherProvider;
import com.amazon.ionhash.MessageDigestIonHasherProvider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Example {
    public static void main(String[] args) throws IOException {
        IonSystem ion = IonSystemBuilder.standard().build();
        IonHasherProvider hasherProvider = new MessageDigestIonHasherProvider("SHA-256");

        // write a simple Ion struct and compute the hash
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IonWriter writer = ion.newTextWriter(baos);
        IonHashWriter hashWriter = IonHashWriterBuilder.standard()
                .withHasherProvider(hasherProvider)
                .withWriter(writer)
                .build();

        System.out.println("writer");
        hashWriter.stepIn(IonType.STRUCT);
          hashWriter.setFieldName("first_name");
          hashWriter.writeString("Amanda");
          hashWriter.setFieldName("middle_name");
          hashWriter.writeString("Amanda");
          hashWriter.setFieldName("last_name");
          hashWriter.writeString("Smith");
        hashWriter.stepOut();
        printBytesHex(hashWriter.digest());

        byte[] data = baos.toByteArray();
        System.out.println("Ion data: " + ion.singleValue(data));

        // cleanup
        hashWriter.close();
        writer.close();
        baos.close();


        // read the struct and compute the hash
        IonReader reader = ion.newReader(data);
        IonHashReader hashReader = IonHashReaderBuilder.standard()
                .withHasherProvider(hasherProvider)
                .withReader(reader)
                .build();

        System.out.println("reader");
        hashReader.next();    // position reader at the first value
        hashReader.next();    // position reader just after the struct
        printBytesHex(hashReader.digest());

        // cleanup
        hashReader.close();
        reader.close();
    }

    private static void printBytesHex(byte[] bytes) {
        for (byte b : bytes) {
            System.out.print(String.format("%02x ", b));
        }
        System.out.println();
    }
}
```
Upon execution, the above code produces the following output:
```
writer
37 82 6e 71 92 a1 e4 e1 24 aa 73 f9 85 0f f1 0f 1c b5 cc ca f2 07 b0 9e 65 af 42 56 ae 8c 80 55 
Ion data: {first_name:"Amanda",middle_name:"Amanda",last_name:"Smith"}
reader
37 82 6e 71 92 a1 e4 e1 24 aa 73 f9 85 0f f1 0f 1c b5 cc ca f2 07 b0 9e 65 af 42 56 ae 8c 80 55 
```
A few items to note:
* IonHashReaders/IonHashWriters allow the caller to retrieve the digest of the value that was just read or written (or "stepped out" of)
* calling digest() on an IonHashReader/IonHashWriter will only return a byte[] containing a digest value if called at the same level at which the IonHashReader/IonHashWriter was created;  otherwise, an empty byte array is returned
* if the caller is only interested in verifying the hash of a container, there is no need to stepIn/iterate/stepOut of the struct;  the caller may simply next past the container, as demonstrated above by this code:
```java
    System.out.println("reader");
    hashReader.next();    // position reader at the first value
    hashReader.next();    // position reader just after the struct
    printBytesHex(hashReader.currentHash());
```
