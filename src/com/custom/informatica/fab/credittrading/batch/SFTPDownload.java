package com.custom.informatica.fab.credittrading.batch;


import com.jcraft.jsch.*;

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
}
