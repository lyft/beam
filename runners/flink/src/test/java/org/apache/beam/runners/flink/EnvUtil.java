package org.apache.beam.runners.flink;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

/**
 * utility class to add / remove environment variables for tests.
 */
public class EnvUtil {

  /**
   * add environment variable using reflection.
   * @param key
   * @param value
   * @throws Exception
   */
  public static void addEnv(String key, String value) throws Exception {

    Class[] klasses = Collections.class.getDeclaredClasses();
    Map<String, String> env = System.getenv();
    for (Class klass : klasses) {
      if ("java.util.Collections$UnmodifiableMap".equals(klass.getName())) {
        for (Field field : klass.getDeclaredFields()) {
          if (field.getType().equals(Map.class)) {
            field.setAccessible(true);
            ((Map<String, String>) field.get(env)).put(key, value);
          }
        }
      }
    }
  }

  /**
   * removes environment variable.
   * @param key
   * @throws Exception
   */
  static void removeEnv(String key) throws Exception {

    Class[] klasses = Collections.class.getDeclaredClasses();
    Map<String, String> env = System.getenv();
    for (Class klass : klasses) {
      if ("java.util.Collections$UnmodifiableMap".equals(klass.getName())) {
        for (Field field : klass.getDeclaredFields()) {
          if (field.getType().equals(Map.class)) {
            field.setAccessible(true);
            ((Map<String, String>) field.get(env)).remove(key);
          }
        }
      }
    }
  }
}
