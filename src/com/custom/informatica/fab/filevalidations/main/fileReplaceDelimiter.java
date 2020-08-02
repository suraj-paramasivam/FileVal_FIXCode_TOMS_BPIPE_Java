package com.custom.informatica.fab.filevalidations.main;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;


public  class fileReplaceDelimiter {

    public static void delimiterChanges(String location, String filename,String oldDelimiter,String newDelimiter){
        File tempfile;
        Set<String> lines = new HashSet<String>();
        String oldDelim  = oldDelimiter;
        String newDelim = newDelimiter;
        String rebuildOldDelim="";
        String rebuildNewDelim = "";
        try{
            tempfile=new File(location+filename);
            FileInputStream fi = new FileInputStream(tempfile);
            DataInputStream di =  new DataInputStream(fi);
            BufferedReader br = new BufferedReader(new InputStreamReader(di));
            String line;

//
//            for(int i=0;i<oldDelim.length();i++){
//                char c=oldDelim.charAt(i);
//                rebuildOldDelim=rebuildOldDelim+"\\\\"+c;
//            }
//
//            for(int i=0;i<newDelim.length();i++){
//                char d = newDelim.charAt(i);
//                rebuildNewDelim=rebuildNewDelim+"\\\\"+d;
//            }
//            System.out.println(rebuildNewDelim);
            while ((line = br.readLine()) != null) {
                line=line.replaceAll(newDelim," ");
                line=line.replaceAll(oldDelim, newDelim);
                lines.add(line);

            }
            br.close();
            BufferedWriter writer = new BufferedWriter(new FileWriter(location+filename));
            for (String l : lines) {
                writer.write(l);
                writer.newLine();
            }
            writer.close();

        }
        catch(Exception e){
            e.printStackTrace();
        }

    }
    public static void main(String[] args){
        delimiterChanges("C:\\Suraj\\Work_Infa\\Projects\\India\\FAB\\scratch\\","testDelimiterChange.csv","\\|\\|","~");
    }
}
