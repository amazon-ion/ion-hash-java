package com.amazon.ionhash.tools;

import com.amazon.ion.IonReader;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ionhash.IonHashReader;
import com.amazon.ionhash.IonHashReaderBuilder;
import com.amazon.ionhash.MessageDigestIonHasherProvider;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Cli {
    public static void main(String[] args) throws Exception {
        String algorithm = args[0];

        InputStream in = System.in;
        if (args.length > 1) {
            in = new FileInputStream(args[1]);
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = br.readLine()) != null) {
            try {
                IonHashReader ihr = IonHashReaderBuilder.standard()
                        .withReader(IonReaderBuilder.standard().build(line))
                        .withHasherProvider(new MessageDigestIonHasherProvider(algorithm))
                        .build();
                ihr.next();
                ihr.next();
                System.out.println(bytesToHex(ihr.digest()));
            } catch (Exception e) {
                System.out.println("[unable to digest: " + e + "]");
            }
        }
        br.close();
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b));
        }
        return sb.toString();
    }
}

