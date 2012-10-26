package net.opentsdb.core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple configuration class consisting of key/value pairs. It's loosely
 * based off of the Hadoop Configuration class and Tsuna's ArgP classes in that
 * it will cast results with a provided default value. This class will be used by
 * the TsdbConfig for loading the config file and parsing values.
 */
public final class ConfigLoader {
  private static final Logger LOG = LoggerFactory
      .getLogger(ConfigLoader.class);

  /** A list of key/values loaded from the configuration file */
  private Properties props = new Properties();

  /** A list of file locations, provided by the user or loads defaults */
  private ArrayList<String> file_locations = null;

  /**
   * Attempts to load a configuration file. If the user called Load(String file)
   * with a file, it will try to load only that file. Otherwise it will attempt
   * to load the file from default locations.
   * @return True if a config file was loaded, false if no file was loaded
   */
  public final boolean loadConfig() {
    if (file_locations == null) {
      file_locations = new ArrayList<String>();

      // search locally first
      file_locations.add("opentsdb.conf");

      // add default locations based on OS
      if (System.getProperty("os.name").toUpperCase().contains("WINDOWS")) {
        file_locations.add("C:\\program files\\opentsdb\\opentsdb.conf");
      } else {
        file_locations.add("/etc/opentsdb.conf");
        file_locations.add("/etc/opentsdb/opentsdb.conf");
        file_locations.add("/opt/opentsdb/opentsdb.conf");
      }
    }

    // loop and load until we get something
    for (String file : file_locations) {
      try {
        props.load(new FileInputStream(file));
        LOG.info("Loaded configuration file [" + file + "]");

        // clear the file_locations so we can load again if necessary
        file_locations.clear();
        file_locations = null;

        return true;
      } catch (FileNotFoundException e) {
        LOG.debug("Unable to load configuration file [" + file + "]");
      } catch (IOException e) {
        LOG.debug("Unable to load configuration file [" + file + "]");
      }
    }

    // couldn't load anything
    LOG.warn("Unable to load any of the configuration files");
    return false;
  }

  /**
   * Attempts to load a configuration file specified by the user
   * @param file The full or relative path to a configuration file
   * @return True if a config file was loaded, false if no file was loaded
   */
  public final boolean loadConfig(String file) {
    if (file.length() > 0) {
      file_locations = new ArrayList<String>();
      file_locations.add(file);
    }

    return loadConfig();
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @return The configuration value as a string
   */
  public final String getString(final String key) {
    String val = props.getProperty(key);
    if (val == null)
      throw new NullPointerException("Could not find key [" + key + "]");
    return val.trim();
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @return The configuration value as an int
   */
  public final int getInt(final String key) {
    String val = props.getProperty(key);
    if (val == null)
      throw new NullPointerException("Could not find key [" + key + "]");
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      throw new NumberFormatException("Unable to convert key [" + key + "] value [" + val
          + "] to an int");
    }
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param min A minimum value for the parameter
   * @param max A maximum value for the parameter
   * @return The configuration value as an int
   */
  public final int getInt(final String key, 
      final int min, final int max) {
    String val = props.getProperty(key);
    if (val == null)
      throw new NullPointerException("Could not find key [" + key + "]");
    try {
      final int temp = Integer.parseInt(val);

      // check min/max
      if (temp < min) {
        throw new NumberFormatException("Key [" + key + "] value [" + temp
            + "] was less than the minimum [" + min + "]");
      }
      if (temp > max) {
        throw new NumberFormatException("Key [" + key + "] value [" + temp
            + "] was greater than the max [" + max + "]");
      }
      return temp;
    } catch (NumberFormatException e) {
      throw new NumberFormatException("Unable to convert key [" + key + "] value [" + val
          + "] to an int");
    }
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @return The configuration value as an short
   */
  public final short getShort(final String key) {
    String val = props.getProperty(key);
    if (val == null)
      throw new NullPointerException("Could not find key [" + key + "]");
    try {
      return Short.parseShort(val);
    } catch (NumberFormatException e) {
      throw new NumberFormatException("Unable to convert key [" + key + "] value [" + val
          + "] to a short");
    }
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param defaultValue The default value to return if the key could not be
   *          found
   * @param min A minimum value for the parameter
   * @param max A maximum value for the parameter
   * @return The configuration value as an short
   */
  public final short getShort(final String key,
      final short defaultValue, final short min, final short max) {
    String val = props.getProperty(key);
    if (val == null)
      throw new NullPointerException("Could not find key [" + key + "]");
    try {
      final short temp = Short.parseShort(val);

      // check min/max
      if (temp < min) {
        throw new NumberFormatException("Key [" + key + "] value [" + temp
            + "] was less than the minimum [" + min + "]");
      }
      if (temp > max) {
        throw new NumberFormatException("Key [" + key + "] value [" + temp
            + "] was greater than the max [" + max + "]");
      }
      return temp;
    } catch (NumberFormatException e) {
      throw new NumberFormatException("Unable to convert key [" + key + "] value [" + val
          + "] to an short");
    }
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @return The configuration value as a long
   */
  public final long getLong(final String key) {
    String val = props.getProperty(key);
    if (val == null)
      throw new NullPointerException("Could not find key [" + key + "]");
    try {
      return Long.parseLong(val);
    } catch (NumberFormatException e) {
      throw new NumberFormatException("Unable to convert key [" + key + "] value [" + val
          + "] to a long");
    }
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param min A minimum value for the parameter
   * @param max A maximum value for the parameter
   * @return The configuration value as an long
   */
  public final long getLong(final String key, 
      final long min, final long max) {
    String val = props.getProperty(key);
    if (val == null)
      throw new NullPointerException("Could not find key [" + key + "]");
    try {
      final long temp = Long.parseLong(val);

      // check min/max
      if (temp < min) {
        throw new NumberFormatException("Key [" + key + "] value [" + temp
            + "] was less than the minimum [" + min + "]");
      }
      if (temp > max) {
        throw new NumberFormatException("Key [" + key + "] value [" + temp
            + "] was greater than the max [" + max + "]");
      }
      return temp;
    } catch (NumberFormatException e) {
      throw new NumberFormatException("Unable to convert key [" + key + "] value [" + val
          + "] to an long");
    }
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @return The configuration value as a float
   */
  public final float getFloat(final String key) {
    String val = props.getProperty(key);
    if (val == null)
      throw new NullPointerException("Could not find key [" + key + "]");
    try {
      return Float.parseFloat(val);
    } catch (NumberFormatException e) {
      throw new NumberFormatException("Unable to convert key [" + key + "] value [" + val
          + "] to a float");
    }
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param min A minimum value for the parameter
   * @param max A maximum value for the parameter
   * @return The configuration value as an float
   */
  public final float getFloat(final String key,
      final float min, final float max) {
    String val = props.getProperty(key);
    if (val == null)
      throw new NullPointerException("Could not find key [" + key + "]");
    try {
      final float temp = Float.parseFloat(val);

      // check min/max
      if (temp < min) {
        throw new NumberFormatException("Key [" + key + "] value [" + temp
            + "] was less than the minimum [" + min + "]");
      }
      if (temp > max) {
        throw new NumberFormatException("Key [" + key + "] value [" + temp
            + "] was greater than the max [" + max + "]");
      }
      return temp;
    } catch (NumberFormatException e) {
      throw new NumberFormatException("Unable to convert key [" + key + "] value [" + val
          + "] to an float");
    }
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @return The configuration value as a boolean
   */
  public final boolean getBoolean(final String key) {
    String val = props.getProperty(key);
    if (val == null)
      throw new NullPointerException("Could not find key [" + key + "]");
    try {
      return Boolean.parseBoolean(val);
    } catch (NumberFormatException e) {
      throw new NumberFormatException("Unable to convert key [" + key + "] value [" + val
          + "] to a boolean");
    }
  }

  /**
   * Sets the value of a key in the configuration properties list
   * @param key The name of the configuration key to modify
   * @param value The value to store for the key
   */
  public final void setConfig(final String key, final String value) {
    if (key.isEmpty()) {
      LOG.warn("Key provided is empty");
      return;
    }
    props.setProperty(key.trim(), value.trim());
  }

  /**
   * Returns all of the configurations stored in the properties list. Note that
   * this is only the properties that have been set via command line or
   * configuration file. Anything not listed will use the defaults
   * @param as_json NOT IMPLEMENTED Returns a JSON string
   * @return A string with all of the settings
   */
  public final String dumpConfiguration(boolean as_json) {
    if (props.isEmpty())
      return "No configuration settings stored";

    // if not asJson, iterate and build a string
    if (as_json) {
      // todo may need to move jsonHelper around to access it here
      // JsonHelper json = new JsonHelper(props);
      return "";
    } else {
      @SuppressWarnings("rawtypes")
      Enumeration e = props.propertyNames();
      String response = "TSD Configuration:\n";
      while (e.hasMoreElements()) {
        String key = (String) e.nextElement();
        response += "Key [" + key + "]  Value [" + props.getProperty(key)
            + "]\n";
      }
      return response;
    }
  }
}
