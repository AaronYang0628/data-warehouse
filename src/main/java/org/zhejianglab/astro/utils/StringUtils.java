package org.zhejianglab.astro.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  public static String listToYamlString(List<?> list) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    if (list != null && !list.isEmpty()) {
      for (var item : list) {
        sb.append(item.toString()).append(", ");
      }
      sb.setLength(sb.length() - 2);
    }
    sb.append("]");
    return sb.toString();
  }

  public static String mapToYamlString(Map<?, ?> map) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    if (map != null && !map.isEmpty()) {
      for (var entry : map.entrySet()) {
        sb.append(entry.getKey()).append(": \"").append(entry.getValue()).append("\", ");
      }
      sb.setLength(sb.length() - 2);
    }
    sb.append("}");
    return sb.toString();
  }

  public static void main(String[] args) {
    System.out.println(mapToYamlString(null));
    System.out.println(mapToYamlString(new HashMap<>()));
    System.out.println(
        mapToYamlString(
            new HashMap<>() {
              {
                put("key1", "value1");
                put("key2", "value2");
              }
            }));

    System.out.println(listToYamlString(null));
    System.out.println(listToYamlString(List.of()));
    System.out.println(listToYamlString(List.of("a", "b", "c")));
  }
}
