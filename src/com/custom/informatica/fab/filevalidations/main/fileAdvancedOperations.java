package com.custom.informatica.fab.filevalidations.main;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class fileAdvancedOperations {
    fileAdvancedOperations fadop = new fileAdvancedOperations();
    private static MessageDigest md;
    private static String vInputFileLocation="C:\\Suraj\\Personal\\Workspaces\\IntelliJ\\Infa_FAB\\testfiles\\";
    private static Map<String,String> fileChecksumList=new HashMap<String,String>();
    private static String checksum = null;

    public fileAdvancedOperations() throws IOException {
    }


    public static byte[] getChecksum(String filename) throws IOException, NoSuchAlgorithmException {
        md = MessageDigest.getInstance("MD5");
        InputStream is = new FileInputStream(filename);
        byte[] buf = new byte[1024];
        int numread;

        do {
            numread = is.read(buf);
            if (numread > 0) {
                md.update(buf, 0, numread);
            }
        } while (numread != -1);
        return md.digest();
    }

    public static String getStringChecksum(String filename) throws IOException, NoSuchAlgorithmException {
        byte[] b = getChecksum(filename);
        String result = "";
        for (int i = 0; i < b.length; i++) {
            result += Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);

        }
        return result;
    }

    public static Map<String,String> execChecksum(List<String> filenamelist) throws IOException, NoSuchAlgorithmException {


        for (String filename : filenamelist) {
            File f = new File(vInputFileLocation+filename);
            if (f.exists()) {
                checksum=getStringChecksum(f.toString());
                fileChecksumList.put(filename, checksum);
            }
            else{continue;}
        }
        return fileChecksumList;
    }

    public static void main(String[] args) throws NoSuchAlgorithmException, IOException {

        List<String> fileList = Arrays.asList("abc.csv","xyz.csv");
//        System.out.println(execChecksum(fileList));
        execChecksum(fileList);
        StringBuilder mapAsString = new StringBuilder();
        for (String key:fileChecksumList.keySet()) {
            mapAsString.append(key + "=" + fileChecksumList.get(key) + ", ");
            mapAsString.delete(mapAsString.length()-2, mapAsString.length()).append("||");

        }
        System.out.println(mapAsString.toString());
    }
//    try(Writer writer = new FileWriter("fileChecksum.csv")) {
//        for (Map.Entry<String, String> entry : fileChecksumList.entrySet()) {
//            writer.append(entry.getKey())
//                    .append(',')
//                    .append(entry.getValue())
//                    .append(eol);
//        }
//    } catch (IOException ex) {
//        ex.printStackTrace(System.err);
//    }
}
