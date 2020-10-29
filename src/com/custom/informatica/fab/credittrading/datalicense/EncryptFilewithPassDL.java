package com.custom.informatica.fab.credittrading.datalicense;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.io.*;
import java.security.GeneralSecurityException;
import java.util.Arrays;

/**
 * This class encrypts a file with a password using PBE(Password Based encryption class of Java) with MD5 and DES. We use a salt of 8 bits. The class also has a method
 * for decryption of the file. The encrypted file is stored as filename+.encrypted in the same folder and the decrypted file is stored as filename+.decrypted.txt
 */

public class EncryptFilewithPassDL {
    public static final Logger logger = Logger.getLogger(EncryptFilewithPassDL.class);

    private static final byte[] salt = {
            (byte) 0x46, (byte) 0x41, (byte) 0x42, (byte) 0x49,
            (byte) 0x4e, (byte) 0x46, (byte) 0x41, (byte) 0x53
    };

    /**
     *
     * @param pass
     * @param decryptMode
     * @return
     * @throws GeneralSecurityException
     */
    private static Cipher makeCipher(String pass, Boolean decryptMode) throws GeneralSecurityException{

        //Use a KeyFactory to derive the corresponding key from the passphrase:
        PBEKeySpec keySpec = new PBEKeySpec(pass.toCharArray());
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
        SecretKey key = keyFactory.generateSecret(keySpec);

        //Create parameters from the salt and an arbitrary number of iterations:

        PBEParameterSpec pbeParamSpec = new PBEParameterSpec(salt, 42);

        /*Dump the key to a file for testing: */
        EncryptFilewithPassDL.keyToFile(key);

        //Set up the cipher:
        Cipher cipher = Cipher.getInstance("PBEWithMD5AndDES");
        logger.info("Using PBEwithMD5andDES to encrypt");

        //Set the cipher mode to decryption or encryption:
        if(decryptMode){
            cipher.init(Cipher.ENCRYPT_MODE, key, pbeParamSpec);
            logger.debug("Setting Encrypt Mode");
        } else {
            cipher.init(Cipher.DECRYPT_MODE, key, pbeParamSpec);
            logger.debug("Setting Decrypt Mode");
        }

        return cipher;
    }

    /**
     *
     * @param key
     */
    private static void keyToFile(SecretKey key){
        try {
            File keyFile = new File("keyfile.txt");
            FileWriter keyStream = new FileWriter(keyFile);
            String encodedKey = "\n" + "Encoded version of key:  " + key.getEncoded().toString();
            logger.debug("Encoded version of key:  " + key.getEncoded().toString());
            keyStream.write(key.toString());
            keyStream.write(encodedKey);
            keyStream.close();
        } catch (IOException e) {
            logger.error("Failure writing key to file");
            logger.error(e);
            System.exit(1);
        }

    }

    /**
     * Encrypts the file with name file name and password pass
     * @param fileName
     * @param pass
     * @throws IOException
     * @throws GeneralSecurityException
     */

    public static void encryptFile(String fileName, String pass)
            throws IOException, GeneralSecurityException{
        byte[] decData;
        byte[] encData;
        File inFile = new File(fileName);
        //Generate the cipher using pass:
        Cipher cipher = EncryptFilewithPassDL.makeCipher(pass, true);

        //Read in the file:
        FileInputStream inStream = new FileInputStream(inFile);

        int blockSize = 8;
        //Figure out how many bytes are padded
        int paddedCount = blockSize - ((int)inFile.length()  % blockSize );

        //Figure out full size including padding
        int padded = (int)inFile.length() + paddedCount;

        decData = new byte[padded];


        inStream.read(decData);

        inStream.close();

        //Write out padding bytes as per PKCS5 algorithm
        for( int i = (int)inFile.length(); i < padded; ++i ) {
            decData[i] = (byte)paddedCount;
        }

        //Encrypt the file data:
        encData = cipher.doFinal(decData);


        //Write the encrypted data to a new file:
        FileOutputStream outStream = new FileOutputStream(new File(fileName + ".encrypted"));
        outStream.write(encData);
        outStream.close();
        logger.info("Created encrypted file");;
    }


    /**
     * Decrypts the file
     * @param fileName
     * @param pass
     * @throws GeneralSecurityException
     * @throws IOException
     */
    public static void decryptFile(String fileName, String pass)
            throws GeneralSecurityException, IOException{
        byte[] encData;
        byte[] decData;
        File inFile = new File(fileName+ ".encrypted");

        //Generate the cipher using pass:
        Cipher cipher = EncryptFilewithPassDL.makeCipher(pass, false);

        //Read in the file:
        FileInputStream inStream = new FileInputStream(inFile );
        encData = new byte[(int)inFile.length()];
        inStream.read(encData);
        inStream.close();
        //Decrypt the file data:
        decData = cipher.doFinal(encData);

        //Figure out how much padding to remove

        int padCount = (int)decData[decData.length - 1];
        
        if( padCount >= 1 && padCount <= 8 ) {
            decData = Arrays.copyOfRange( decData , 0, decData.length - padCount);
        }

        //Write the decrypted data to a new file:



        FileOutputStream target = new FileOutputStream(new File(fileName + ".decrypted.txt"));
        target.write(decData);
        logger.info("Created decrypted file");
        target.close();
    }

    /**
     * Argument 1 - Credential file with Client id and secret as json
     * Argument 2 - Password to encrypt the file
     * @param args
     */
    public static void main(String[] args){
        PropertyConfigurator.configure("res/encrypt_file_dl_logging.properties");
        String filename = args[0];
        String password=args[1];
        try{
            encryptFile(filename,password);

        }
        catch(IOException | GeneralSecurityException e){
            e.printStackTrace();
            logger.error(e);
            System.exit(1);
        }
    }
}
