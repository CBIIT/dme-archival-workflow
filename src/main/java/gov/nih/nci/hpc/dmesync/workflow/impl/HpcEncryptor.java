/*******************************************************************************
 * Copyright SVG, Inc.
 * Copyright Leidos Biomedical Research, Inc.
 *  
 * Distributed under the OSI-approved BSD 3-Clause License.
 * See https://github.com/CBIIT/HPC_DME_APIs/LICENSE.txt for details.
 ******************************************************************************/

package gov.nih.nci.hpc.dmesync.workflow.impl;

import gov.nih.nci.hpc.domain.error.HpcErrorType;
import gov.nih.nci.hpc.exception.HpcException;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class HpcEncryptor
{ 
    //---------------------------------------------------------------------//
    // Instance members
    //---------------------------------------------------------------------//
	
	// Ciphers.
	private Cipher encryptCipher = null;
	private Cipher decryptCipher = null;
	private String salt;
	
	// The logger instance.
	private final Logger logger = 
			             LoggerFactory.getLogger(this.getClass().getName());
	
    //---------------------------------------------------------------------//
    // Constructors
    //---------------------------------------------------------------------// 
    
    /**
     * Constructor for Spring Dependency Injection.
     * 
     * @param key The encryption key.
     * @throws HpcException If an encryption key was not provided.
     */
    private HpcEncryptor(@Value("${dmesync.encryptor.key:}") String key, @Value("${dmesync.encryptor.salt:}") String salt) throws HpcException
    {
    	if(key == null || key.isEmpty()) {
        	  SecureRandom random = new SecureRandom();
        	  byte bytes[] = new byte[16];
        	  random.nextBytes(bytes);
        	  key = new String(bytes);
    	}
    	
    	Key aesKey = new SecretKeySpec(key.getBytes(), "AES");
    	try {
    		 encryptCipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
    		 encryptCipher.init(Cipher.ENCRYPT_MODE, aesKey);
    		 
    		 decryptCipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
    		 decryptCipher.init(Cipher.DECRYPT_MODE, aesKey);
    		 
    	} catch(Exception e) {
    		    throw new HpcException("Failed to instantiate an AES Cipher", 
    		    	                   HpcErrorType.UNEXPECTED_ERROR, e);
    	}
    	this.salt = salt;
    }
    
    //---------------------------------------------------------------------//
    // Methods
    //---------------------------------------------------------------------//
    
    /**
     * Encrypt text.
     * 
     * @param text The text to encrypt.
     * @return The encrypted text.
     */
	public byte[] encrypt(String text)
	{
		try {
		     return encryptCipher.doFinal(text.getBytes());
		     
		} catch(Exception e) {
			    logger.error("Failed to encrypt: {}", e.getMessage());
		}
		
		return null;    
	}
 
    /**
     * Decrypt text.
     * 
     * @param binary The binary to decrypt.
     * @return The decrypted text.
     */
	public String decrypt(byte[] binary) 
	{
		try {
		     return new String(decryptCipher.doFinal(binary));
		     
		} catch(Exception e) {
			    logger.error("Failed to decrypt: {}" + e.getMessage());
		}
		
		return null; 
	}
	
    public String digest(String message) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(message.getBytes(StandardCharsets.UTF_8));
        byte[] hash = digest.digest(salt.getBytes(StandardCharsets.UTF_8));
        return bytesToHex(hash);
    }
  
    private static String bytesToHex(byte[] hashInBytes) {
  
        StringBuilder sb = new StringBuilder();
        for (byte b : hashInBytes) {
          sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}

 