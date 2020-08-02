package com.custom.informatica.fab.credittrading.common;
import java.security.*;
import java.io.File;

import javax.crypto.Cipher;
public class EncryptPassword {
public static void main(String[] args) throws NoSuchAlgorithmException {
    Signature s = Signature.getInstance("SHA256withRSA");
    KeyPairGenerator kp = KeyPairGenerator.getInstance("RSA");
    kp.initialize(2048);
    KeyPair keyPair  = kp.generateKeyPair();
    PublicKey publicKey = keyPair.getPublic();
    System.out.println(publicKey);

}
public static void writeFile(String filename, byte[] data){
    File f=  new File(filename);
}
}
