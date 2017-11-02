# Amazon Ion Hash Java
An implementation of [Amazon Ion Hash](http://amzn.github.io/ion-hash-spec) in Java.

[![Build Status](https://travis-ci.org/amzn/ion-hash-java.svg?branch=master)](https://travis-ci.org/amzn/ion-hash-java)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.amazon.ionhash/ion-hash-java/badge.svg)](https://maven-badges.herokuapp.com/maven-central/software.amazon.ionhash/ion-hash-java)
[![Javadoc](https://javadoc-emblem.rhcloud.com/doc/software.amazon.ionhash/ion-hash-java/badge.svg)](http://www.javadoc.io/doc/software.amazon.ionhash/ion-hash-java)

## Depending on the Library
To start using `ion-hash-java` in your code with Maven, insert the following
dependency into your project's `pom.xml`, replacing ${version} with the
desired version of the library (e.g., 1.0.0):

```xml
<dependency>
  <groupId>software.amazon.ionhash</groupId>
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

import software.amazon.ion.IonReader;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.system.IonSystemBuilder;
import software.amazon.ionhash.IonHashReader;
import software.amazon.ionhash.IonHashReaderBuilder;
import software.amazon.ionhash.IonHashWriter;
import software.amazon.ionhash.IonHashWriterBuilder;
import software.amazon.ionhash.IonHasherProvider;
import software.amazon.ionhash.MessageDigestIonHasherProvider;

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
          printBytesHex(hashWriter.currentHash());
          hashWriter.setFieldName("middle_name");
          hashWriter.writeString("Amanda");
          printBytesHex(hashWriter.currentHash());
          hashWriter.setFieldName("last_name");
          hashWriter.writeString("Smith");
          printBytesHex(hashWriter.currentHash());
        hashWriter.stepOut();
        printBytesHex(hashWriter.currentHash());

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
        hashReader.stepIn();
          hashReader.next();  // position reader at the first value
          hashReader.next();  // position reader just after first value
          printBytesHex(hashReader.currentHash());
          hashReader.next();
          printBytesHex(hashReader.currentHash());
          hashReader.next();
          printBytesHex(hashReader.currentHash());
        hashReader.stepOut();
        printBytesHex(hashReader.currentHash());

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
6b d6 32 23 7a 76 53 8e ff dd 2b 70 03 e5 c9 67 fa 58 74 8b 82 1e ea 7b 9e ad 8c e5 0e ba 95 c3 
6b d6 32 23 7a 76 53 8e ff dd 2b 70 03 e5 c9 67 fa 58 74 8b 82 1e ea 7b 9e ad 8c e5 0e ba 95 c3 
4f 0a 04 4b 94 ce 84 7d 79 29 a5 92 b9 7b fd 72 d3 c7 cc d1 64 26 ec 18 ee 84 dc 2f 57 a9 71 6d 
44 07 fa 14 fe fb 1e ec f5 7f 51 b3 af 97 ec e6 c2 09 11 9d 18 0c bd b1 88 07 a7 ab f9 7d 3d cc 
Ion data: {first_name:"Amanda",middle_name:"Amanda",last_name:"Smith"}
reader
6b d6 32 23 7a 76 53 8e ff dd 2b 70 03 e5 c9 67 fa 58 74 8b 82 1e ea 7b 9e ad 8c e5 0e ba 95 c3 
6b d6 32 23 7a 76 53 8e ff dd 2b 70 03 e5 c9 67 fa 58 74 8b 82 1e ea 7b 9e ad 8c e5 0e ba 95 c3 
4f 0a 04 4b 94 ce 84 7d 79 29 a5 92 b9 7b fd 72 d3 c7 cc d1 64 26 ec 18 ee 84 dc 2f 57 a9 71 6d 
44 07 fa 14 fe fb 1e ec f5 7f 51 b3 af 97 ec e6 c2 09 11 9d 18 0c bd b1 88 07 a7 ab f9 7d 3d cc 
```
A few items to note:
* IonHashReaders/IonHashWriters allow the caller to retrieve the hash of the value that was just read or written (or "stepped out" of)
* if the caller is only interested in verifying the hash of a container, there is no need to stepIn/iterate/stepOut of the struct;  the caller may simply next past the container, as in:
```java
    System.out.println("reader");
    hashReader.next();    // position reader at the first value
    hashReader.next();    // position reader just after the struct
    printBytesHex(hashReader.currentHash());
```
```
  would output:
    reader
    44 07 fa 14 fe fb 1e ec f5 7f 51 b3 af 97 ec e6 c2 09 11 9d 18 0c bd b1 88 07 a7 ab f9 7d 3d cc 
```
* while struct field names are used when computing the hash of the containing struct, they are NOT represented in the hash immediately after a field is read or written (in the above example, note that the hashes of the first_name:"Amanda" and middle_name:"Amanda" fields are identical)
