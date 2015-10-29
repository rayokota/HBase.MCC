package org.apache.hadoop.hbase.client;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;

public class HBaseMultiClusterConfigUtil {
  
  static Logger LOG = Logger.getLogger(HBaseMultiClusterConfigUtil.class);
  static final String PRIMARY_NAME = "primary";
  
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("HBaseMultiClusterUtil <command> <args ....>");
      System.out.println("HBaseMultiClusterUtil combineConfigs <primary conf 1> <failover name> <failover conf 1> <optional failover name N> <optional failover conf N>");
      System.out.println("HBaseMultiClusterUtil splitConfigs <configuration file>");
      System.out.println("HBaseMultiClusterUtil combinConfigsFromCM <host1> <user1> <pwd1> <cluster1> <hbaseName1> <host2> <user2> <pwd2> <cluster2> <hbaseName2> <outputFile>");
      return;
    }
    System.out.println("Command: " + args[0]);
    if (args[0].equals("combineConfigs")) {
      String outputFile = args[1];
      
      Configuration primaryConfig = generateCombinedConfig(args);
      
      LOG.info("Writting Out New Primary");
      primaryConfig.writeXml(new BufferedWriter(new FileWriter(new File(outputFile))));
      LOG.info(" - Successful Written Out New Primary");
    } else if (args[0].equals("splitConfigs")) {
      
      Configuration config = HBaseConfiguration.create();
      config.addResource(new FileInputStream(new File(args[1])));
      
      OutputStream ops2 = new StringOutputStream();
      
      config.writeXml(ops2);
      //System.out.println(ops2.toString());
      
      splitMultiConfigFile(config);
    } else if (args[0].equals("splitConfigs")) {
      LOG.info("Unknown command: " + args[0]);
    }
  }
  
  /**
   * This method will take a multi hbase config and produce all the single config files
   * @param config
   * @return
   */
  public static Map<String, Configuration> splitMultiConfigFile(Configuration config) {
    
    Map<String, Configuration> results = new HashMap<String, Configuration>();
    
    Collection<String> failoverNames = config.getStringCollection(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG);
    
    System.out.println("FailoverNames: " + failoverNames.size() + " " + config.get(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG));
    if (failoverNames.size() == 0) {
      results.put(PRIMARY_NAME, config);
    } else {
      
      // add failover configs
      for (String failoverName: failoverNames) {
        System.out.println("spliting: " + failoverName);
        Configuration failoverConfig = new Configuration(config);
        
        results.put(failoverName, failoverConfig);
        
        failoverConfig.unset(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG);
        
        Iterator<Entry<String, String>> it = failoverConfig.iterator();
        while (it.hasNext()) {
          Entry<String, String> keyValue = it.next();
          if (keyValue.getKey().startsWith(ConfigConst.HBASE_FAILOVER_CLUSTER_CONFIG)) {
            //System.out.println("adding: " + failoverName + " " + keyValue.getKey() + " " + keyValue.getValue());
            failoverConfig.set(keyValue.getKey().substring(ConfigConst.HBASE_FAILOVER_CLUSTER_CONFIG.length() + 2 + failoverName.length()), keyValue.getValue());
            failoverConfig.unset(keyValue.getKey());
          }
        }
      }
      
      //clean up primary config
      Iterator<Entry<String, String>> it = config.iterator();
      //config.unset(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG);
      while (it.hasNext()) {
        Entry<String, String> keyValue = it.next();
        if (keyValue.getKey().startsWith(ConfigConst.HBASE_FAILOVER_CLUSTER_CONFIG)) {
          //config.unset(keyValue.getKey());
        }
      }
      results.put(PRIMARY_NAME, config);
    }

    //print configs
    //
    /*
    for (Entry<String, Configuration> entry: results.entrySet()) {
      System.out.println("Config: " + entry.getKey());
      OutputStream ops = new StringOutputStream();
      try {
        entry.getValue().writeXml(ops);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      System.out.println(ops.toString());
    }
    */
    
    return results;
  }
  
  /**
   * This method will take args and up load config files to produce a single
   * multi hbase cluster config file.
   * 
   * The args should be like this.
   * 
   * args[0] commandName (not used here)
   * args[1] output file (not used here)
   * args[2] primary file
   * args[3] failover 1 name
   * args[4] failover 1 file
   * args[3+N*2] optional failover N name
   * args[4+N*2] optional failover N file
   * @param args
   * @return
   * @throws IOException
   */
  public static Configuration generateCombinedConfig(String[] args) throws IOException {
    String primaryConfigFile = args[2];
    HashMap<String, String> failoverConfigNameAndFiles = new HashMap<String, String>();
    for (int i = 3; i < args.length; i+=2) {
      failoverConfigNameAndFiles.put(args[i], args[i+1]);
    }
    return generateCombinedConfig(primaryConfigFile, failoverConfigNameAndFiles);
  }
  
  /**
   * This method will load config files to produce a single
   * multi hbase cluster config file.
   * 
   * The args should be like this.
   * 
   */
  public static Configuration generateCombinedConfig(String primaryConfigFile, Map<String, String> failoverConfigNameAndFiles) throws IOException {

    Configuration primaryConfig = HBaseConfiguration.create();
    primaryConfig.addResource(primaryConfigFile);

    HashMap<String, Configuration> failoverMap = new HashMap<String, Configuration>();
    for (Entry<String, String> entry: failoverConfigNameAndFiles.entrySet()) {
      Configuration failureConfig = HBaseConfiguration.create();
      failureConfig.addResource(entry.getValue());
      failoverMap.put(entry.getKey(), failureConfig);
    }
    return combineConfigurations(primaryConfig, failoverMap);
  }

  public static Configuration combineConfigurations(Configuration primary, Configuration failover ) {
    Map<String, Configuration> map = new HashMap<String, Configuration>();
    map.put("failover", failover);
    return combineConfigurations(primary, map);
  }


  public static Configuration combineConfigurations(Configuration primary, Map<String, Configuration> failovers ) {

    Configuration resultingConfig = new Configuration();
    resultingConfig.clear();

    boolean isFirst = true;
    StringBuilder failOverClusterNames = new StringBuilder();


    Iterator<Entry<String, String>> primaryIt =  primary.iterator();

    while(primaryIt.hasNext()) {
      Entry<String, String> primaryKeyValue = primaryIt.next();
      resultingConfig.set(primaryKeyValue.getKey().replace('_', '.'), primaryKeyValue.getValue());
    }


    for (Entry<String, Configuration> failover: failovers.entrySet()) {
      if (isFirst) {
        isFirst = false;
      } else {
        failOverClusterNames.append(",");
      }
      failOverClusterNames.append(failover.getKey());

      Configuration failureConfig = failover.getValue();

      Iterator<Entry<String, String>> it = failureConfig.iterator();
      while (it.hasNext()) {
        Entry<String, String> keyValue = it.next();

        LOG.info(" -- Looking at : " + keyValue.getKey() + "=" + keyValue.getValue());

        String configKey = keyValue.getKey().replace('_', '.');

        if (configKey.startsWith("hbase.")) {
          resultingConfig.set(ConfigConst.HBASE_FAILOVER_CLUSTER_CONFIG + "." + failover.getKey() + "." + configKey , keyValue.getValue());
          LOG.info(" - Porting config: " + configKey + "=" + keyValue.getValue());
        }
      }
    }

    resultingConfig.set(ConfigConst.HBASE_FAILOVER_CLUSTERS_CONFIG, failOverClusterNames.toString());

    return resultingConfig;
  }
  
  public static class StringOutputStream extends OutputStream {

    private StringBuilder string = new StringBuilder();
    @Override
    public void write(int b) throws IOException {
        this.string.append((char) b );
    }

    //Netbeans IDE automatically overrides this toString()
    public String toString(){
        return this.string.toString();
    }    
  }
}
