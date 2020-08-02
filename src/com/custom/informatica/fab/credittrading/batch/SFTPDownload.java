package com.custom.informatica.fab.credittrading.batch;


import com.jcraft.jsch.*;
import com.jcraft.jsch.KeyPair;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

public class SFTPDownload {
    String SFTPHost = "hostIP";
    int port = 22;
    String username="test";
    String password="test";
    String transferToDirectory = "/tmp";
    Session session = null;

    //Connection to Host
    public void connect() throws JSchException{
        JSch jsch = new JSch();
        session = jsch.getSession(username,SFTPHost,port);
        session.setPassword(password);
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
        }
        catch(Exception e){
            e.printStackTrace();
        }

    }
    public static PublicKey getPublicKey(String filename) throws Exception {

            byte[] keyBytes = Files.readAllBytes(Paths.get(filename));

            X509EncodedKeySpec spec =
                    new X509EncodedKeySpec(keyBytes);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return kf.generatePublic(spec);
    }

    public static PrivateKey getPrivateKey(String filename) throws Exception {

        byte[] keyBytes = Files.readAllBytes(Paths.get(filename));

        X509EncodedKeySpec spec =
                new X509EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(spec);
    }


}
