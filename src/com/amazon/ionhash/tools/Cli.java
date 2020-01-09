/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.ionhash.tools;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ionhash.IonHashReader;
import com.amazon.ionhash.IonHashReaderBuilder;
import com.amazon.ionhash.MessageDigestIonHasherProvider;

import java.io.FileInputStream;
import java.io.InputStream;

public class Cli {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Utility that prints the Ion Hash of the top-level values in a file.");
            System.out.println();
            System.out.println("Usage:");
            System.out.println("  ion-hash [algorithm] [filename]");
            System.out.println();
            System.out.println("where [algorithm] is a hash function such as sha-256");
            System.out.println();
            System.exit(1);
        }

        String algorithm = args[0];
        String fileName = args[1];

        try (InputStream inputStream = new FileInputStream(fileName);
             IonReader reader = IonReaderBuilder.standard().build(inputStream);
             IonHashReader hashReader = IonHashReaderBuilder.standard()
                     .withReader(reader)
                     .withHasherProvider(new MessageDigestIonHasherProvider(algorithm))
                     .build()) {

            IonType type = hashReader.next();
            while (type != null) {
                try {
                    type = hashReader.next();
                    System.out.println(bytesToHex(hashReader.digest()));
                } catch (Exception e) {
                    System.out.println("[unable to digest: " + e + "]");
                }
            }
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b));
        }
        return sb.toString().trim();
    }
}

