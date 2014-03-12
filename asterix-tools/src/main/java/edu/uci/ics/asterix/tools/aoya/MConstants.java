package edu.uci.ics.asterix.tools.aoya;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants used in both Client and Application Master
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MConstants {
  public static final String CC = "CC";
  public static final String NC = "NC";
  public static final String IODEVICES = "IODEVICES";
  /**
   * Environment key name pointing to the the app master jar location
   */
  public static final String APPLICATIONMASTERJARLOCATION = "APPLICATIONMASTERJARLOCATION";

  /**
   * Environment key name denoting the file timestamp for the the app master jar.
   * Used to validate the local resource.
   */
  public static final String APPLICATIONMASTERJARTIMESTAMP = "APPLICATIONMASTERJARTIMESTAMP";

  /**
   * Environment key name denoting the file content length for the app master jar.
   * Used to validate the local resource.
   */
  public static final String APPLICATIONMASTERJARLEN = "APPLICATIONMASTERJARLEN";
  /**
   * Environment key name pointing to the runnable jar location
   */
  public static final String LIBSLOCATION = "LIBSLOCATION";

  /**
   * Environment key name denoting the file timestamp for the runnable jar.
   * Used to validate the local resource.
   */
  public static final String LIBSTIMESTAMP = "LIBSTIMESTAMP";

  /**
   * Environment key name denoting the file content length for the runnable jar
   * Used to validate the local resource.
   */
  public static final String LIBSLEN = "LIBSCRIPTLEN";
}
