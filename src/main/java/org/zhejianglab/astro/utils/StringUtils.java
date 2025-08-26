package org.zhejianglab.astro.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class StringUtils {

  public static final String EMPTY = "";

  public static String generateMD5(String objJsonStr) {
    try {

      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] messageDigest = md.digest(objJsonStr.getBytes());

      BigInteger no = new BigInteger(1, messageDigest);
      String hashtext = no.toString(16);

      while (hashtext.length() < 32) {
        hashtext = "0" + hashtext;
      }

      return hashtext.substring(0, 20);
    } catch (NoSuchAlgorithmException e) {
      return "00000000000000000000";
    }
  }
}
