package com.custom.informatica.fab.credittrading.batch;


import com.jcraft.jsch.*;
import org.apache.commons.io.FileUtils;

import javax.crypto.Cipher;
import java.io.*;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import java.util.logging.Logger;
import java.util.logging.FileHandler;

public class SFTPDownload {
    String SFTPHost = "test.rebex.net";
    int port = 22;
    String username="demo";
    String password="password";
    String transferToDirectory = "/tmp";
    Session session = null;
    String ProxyName = "10.163.0.84";
    int ProxyPort = 8080;

    FileHandler handler = new FileHandler("SFTPDownload.log", true);
    Logger logger =  Logger.getLogger("com.custom.informatica.fab.credittrading.batch");


    public SFTPDownload() throws IOException {
        logger.addHandler(handler);

    }

    //Connection to Host
    public void connect() throws Exception {
        JSch jsch = new JSch();
        session = jsch.getSession(username,SFTPHost,port);
        String passwordExtract = decryptPassword("passwordFile","test");
        session.setPassword(passwordExtract);
        session.setConfig("StrictHostKeyChecking","no");
        session.setProxy(new ProxyHTTP(ProxyName, ProxyPort));
        try{
        session.connect();}
        catch(Exception e){
            logger.severe("Exception occurred during connection, StackTrace is : "+e);
        }
    }

    //Download files
    public void download(String source, String destination) throws JSchException, SftpException {
        Channel channel = session.openChannel("sftp");
        channel.connect();
        ChannelSftp sftpChannel = (ChannelSftp) channel;
        try{
        sftpChannel.get(source, destination);
        sftpChannel.exit();}
        catch(SftpException se){
            logger.severe("Error occurred in SFTP, Stack Trace is : "+ se);
        }
        catch(Exception e){
            logger.severe("Error occurred during other activity, Stack Trace is: "+e);
        }
    }

    //Disconnect
    public void disconnect() {
        if (session != null) {
            session.disconnect();
        }
    }

    public static void main(String[] args) throws IOException {
        SFTPDownload sftp = new SFTPDownload();
        try{
            sftp.connect();
            sftp.download("/pub/example/readme.txt",".");
            System.out.println("Download Completed");
            sftp.logger.info("Completed Download");
            sftp.disconnect();
            sftp.logger.info("Disconnected");
        }
        catch(Exception e){
            e.printStackTrace();
            sftp.logger.severe("An Exception occurred: "+e);
        }

    }
    public static PublicKey getPublicKey(String filename) throws Exception {

        File f1 = new File(filename + "_public.key");
        SFTPDownload sftp1= new SFTPDownload();
        FileInputStream fis = new FileInputStream(f1);
        DataInputStream dis = new DataInputStream(fis);
        byte[] keyBytes = new byte[(int) f1.length()];
        dis.readFully(keyBytes);
        X509EncodedKeySpec spec =
                new X509EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        sftp1.logger.info("Public key generated to file: "+filename+"_public.key");
        return kf.generatePublic(spec);
    }

    public static PrivateKey getPrivateKey(String filename) throws Exception {
        File f1 = new File(filename + "_private.key");
        SFTPDownload sftp1= new SFTPDownload();
        FileInputStream fis = new FileInputStream(f1);
        DataInputStream dis = new DataInputStream(fis);
        byte[] keyBytes = new byte[(int) f1.length()];
        dis.readFully(keyBytes);
        PKCS8EncodedKeySpec spec =
                new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        sftp1.logger.info("Public key generated to file: "+filename+"_private.key");
        return kf.generatePrivate(spec);
    }

    public static String decryptPassword(String passwordFile,String privatekeyFile) throws Exception {
        PrivateKey prk = getPrivateKey(privatekeyFile);
        SFTPDownload sftp1= new SFTPDownload();
        javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("RSA");
        cipher.init(Cipher.DECRYPT_MODE, prk);
        File f1 = new File(passwordFile);
        byte[] fis1 = FileUtils.readFileToByteArray(f1);

        byte[] decryptedTextPassword = cipher.doFinal(fis1);
        sftp1.logger.info("Password Decrypted to :"+new String(decryptedTextPassword));
        return new String(decryptedTextPassword);
    }

}
