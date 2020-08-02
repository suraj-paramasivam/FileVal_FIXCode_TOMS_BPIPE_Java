package com.custom.informatica.fab.credittrading.batch;


import com.jcraft.jsch.*;
import org.apache.commons.io.FileUtils;

import javax.crypto.Cipher;
import java.io.*;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

public class SFTPDownload {
    String SFTPHost = "test.rebex.net";
    int port = 22;
    String username="demo";
    String password="password";
    String transferToDirectory = "/tmp";
    Session session = null;

    //Connection to Host
    public void connect() throws Exception {
        JSch jsch = new JSch();
        session = jsch.getSession(username,SFTPHost,port);
        String passwordExtract = decryptPassword("passwordFile","test");
        session.setPassword(passwordExtract);
        session.setConfig("StrictHostKeyChecking","no");
        session.connect();
    }

    //Download files
    public void download(String source, String destination) throws JSchException, SftpException {
        Channel channel = session.openChannel("sftp");
        channel.connect();
        ChannelSftp sftpChannel = (ChannelSftp) channel;
        sftpChannel.get(source, destination);
        sftpChannel.exit();
    }

    //Disconnect
    public void disconnect() {
        if (session != null) {
            session.disconnect();
        }
    }

    public static void main(String[] args){
        SFTPDownload sftp = new SFTPDownload();
        try{
            sftp.connect();
            sftp.download("/pub/example/readme.txt",".");
            System.out.println("Download Completed");
            sftp.disconnect();
        }
        catch(Exception e){
            e.printStackTrace();
        }

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

    public static String decryptPassword(String passwordFile,String privatekeyFile) throws Exception {
        PrivateKey prk = getPrivateKey(privatekeyFile);
        javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("RSA");
        cipher.init(Cipher.DECRYPT_MODE, prk);
        File f1 = new File(passwordFile);
        byte[] fis1 = FileUtils.readFileToByteArray(f1);

        byte[] decryptedTextPassword = cipher.doFinal(fis1);
        return new String(decryptedTextPassword);
    }

}
