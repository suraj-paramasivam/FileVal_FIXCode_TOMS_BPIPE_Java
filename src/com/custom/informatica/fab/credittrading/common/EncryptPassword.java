package com.custom.informatica.fab.credittrading.common;

import org.apache.commons.io.FileUtils;

import javax.crypto.Cipher;
import java.io.*;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

public class EncryptPassword {
    public static void main(String[] args) throws Exception {
        System.out.println("This program encrypts and decrypts password. Parameters to be passed: Password , Password File where encrypted password is stored, Location+prefix of public and private keys");
        Signature s = Signature.getInstance("SHA256withRSA");
        KeyPairGenerator kp = KeyPairGenerator.getInstance("RSA");
        kp.initialize(2048);
        KeyPair keyPair = kp.generateKeyPair();
        writeKeyPair(args[2], keyPair);
        encryptPassword(args[0], args[1]);
        decryptPassword("passwordFile");

    }

    public static void writeKeyPair(String prefixFilename, KeyPair keyPair) throws IOException {
        PrivateKey privateKey = keyPair.getPrivate();
        PublicKey publicKey = keyPair.getPublic();
        //Store Public Key in 509 encoded spec
        X509EncodedKeySpec x509EncodedKeySpec = new X509EncodedKeySpec(publicKey.getEncoded());
        File f = new File(prefixFilename + "_public.key");
        FileOutputStream fw = new FileOutputStream(f);
        fw.write(x509EncodedKeySpec.getEncoded());
        fw.close();
        //Store Private Key in pkcs encoded spec
        PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec(privateKey.getEncoded());
        File f1 = new File(prefixFilename + "_private.key");
        FileOutputStream fpr = new FileOutputStream(f1);
        fpr.write(pkcs8EncodedKeySpec.getEncoded());
        fpr.close();

    }

    public static PublicKey getPublicKey(String filename) throws Exception {

        File f1 = new File(filename + "_public.key");
        FileInputStream fis = new FileInputStream(f1);
        DataInputStream dis = new DataInputStream(fis);
        byte[] keyBytes = new byte[(int) f1.length()];
        dis.readFully(keyBytes);
        X509EncodedKeySpec spec =
                new X509EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePublic(spec);
    }

    public static PrivateKey getPrivateKey(String filename) throws Exception {
        File f1 = new File(filename + "_private.key");
        FileInputStream fis = new FileInputStream(f1);
        DataInputStream dis = new DataInputStream(fis);
        byte[] keyBytes = new byte[(int) f1.length()];
        dis.readFully(keyBytes);
        PKCS8EncodedKeySpec spec =
                new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(spec);
    }


    public static void encryptPassword(String password, String passwordFile) throws Exception {
        PublicKey pk = getPublicKey("test");
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, pk);
        byte[] pass = password.getBytes();
        cipher.update(pass);
        byte[] encryptedPassword = cipher.doFinal();
        FileOutputStream passFile = new FileOutputStream(new File(passwordFile));
        passFile.write(encryptedPassword);
    }

    public static void decryptPassword(String passwordFile) throws Exception {
        PrivateKey prk = getPrivateKey("test");
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.DECRYPT_MODE, prk);
        File f1 = new File(passwordFile);
        byte[] fis1 = FileUtils.readFileToByteArray(f1);

        byte[] decryptedTextPassword = cipher.doFinal(fis1);
        System.out.println(new String(decryptedTextPassword));
    }
}
