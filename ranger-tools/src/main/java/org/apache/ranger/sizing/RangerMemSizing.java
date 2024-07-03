/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.sizing;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServicePolicies.SecurityZoneInfo;
import org.apache.ranger.plugin.util.ServiceTags;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class RangerMemSizing {
  private static final String OPT_MODE_SPACE      = "space";
  private static final String OPT_MODEL_RETRIEVAL = "retrieval";

  private final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

  private final Gson        gson;
  private final String      policyFile;
  private final String      tagFile;
  private final String      rolesFile;
  private final String      userStoreFile;
  private final String      gdsInfoFile;
  private final boolean     deDup;
  private final boolean     deDupStrings;
  private final String      optimizationMode;
  private final PrintStream out;

  public RangerMemSizing(CommandLine cmdLine) {
    this.out              = System.out;
    this.gson             = createGson();
    this.policyFile       = cmdLine.getOptionValue('p');
    this.tagFile          = cmdLine.getOptionValue('t');
    this.rolesFile        = cmdLine.getOptionValue('r');
    this.userStoreFile    = cmdLine.getOptionValue('u');
    this.gdsInfoFile      = cmdLine.getOptionValue('g');
    this.deDup            = Boolean.parseBoolean(cmdLine.getOptionValue("d", "true"));
    this.deDupStrings     = this.deDup;
    this.optimizationMode = StringUtils.startsWithIgnoreCase(cmdLine.getOptionValue('o', "space"), "s") ? OPT_MODE_SPACE : OPT_MODEL_RETRIEVAL;
  }

  public void run() {
    PerfMemTimeTracker tracker = new PerfMemTimeTracker("RangerMemSizing");

    ServicePolicies  policies  = loadPolicies(policyFile, tracker);
    ServiceTags      tags      = loadTags(tagFile, tracker);
    RangerRoles      roles     = loadRoles(rolesFile, tracker);
    RangerUserStore  userStore = loadUserStore(userStoreFile, tracker);
    ServiceGdsInfo   gdsInfo   = loadGdsInfo(gdsInfoFile, tracker);
    RangerBasePlugin plugin    = createRangerPlugin(policies, tags, roles, userStore, gdsInfo, tracker);

    tracker.stop();

    out.println();
    out.println("Parameters:");
    if (policies != null) {
      out.println("  Policies:  file=" + policyFile + ", size=" + new File(policyFile).length() + ", " + toSummaryStr(policies));
    }

    if (tags != null) {
      out.println("  Tags:      file=" + tagFile + ", size=" + new File(tagFile).length() + ", " + toSummaryStr(tags));
    }

    if (roles != null) {
      out.println("  Roles:     file=" + rolesFile + ", size=" + new File(rolesFile).length() + ", " + toSummaryStr(roles));
    }

    if (userStore != null) {
      out.println("  UserStore: file=" + userStoreFile + ", size=" + new File(userStoreFile).length() + ", " + toSummaryStr(userStore));
    }

    out.println("  DeDup:     " + deDup);
    out.println("  OptMode:   " + optimizationMode);
    out.println();

    out.println("Results:");
    out.println("*****************************");
    tracker.print(out, true);
    out.println("*****************************");
  }

  public static void main(String[] args) {
    CommandLine cmdLine = parseArgs(args);

    if (cmdLine != null) {
      RangerMemSizing memSizing = new RangerMemSizing(cmdLine);

      memSizing.run();
    }
  }

  private ServicePolicies loadPolicies(String fileName, PerfMemTimeTracker parent) {
    if (fileName == null) {
      return null;
    }

    ServicePolicies ret = null;

    try {
      File               file        = new File(fileName);
      PerfMemTimeTracker loadTracker = new PerfMemTimeTracker("Load policies");

      log("loading policies(file=" + fileName + ")");

      {
        PerfMemTimeTracker tracker = new PerfMemTimeTracker("Read policies");

        try (FileReader reader = new FileReader(file)) {
          ret = gson.fromJson(reader, ServicePolicies.class);
        }

        tracker.stop();
        loadTracker.addChild(tracker);
      }

      if (deDupStrings) {
        PerfMemTimeTracker tracker = new PerfMemTimeTracker("DeDupStrings");

        ret.dedupStrings();

        tracker.stop();
        loadTracker.addChild(tracker);
      }

      loadTracker.stop();
      parent.addChild(loadTracker);

      log("loaded policies(file=" + fileName + ", size=" + file.length() + "): " + toSummaryStr(ret));
    } catch (FileNotFoundException excp) {
      log(fileName + ": file does not exist!");
    } catch (IOException excp) {
      log(fileName, excp);
    }

    return ret;
  }

  private ServiceTags loadTags(String fileName, PerfMemTimeTracker parent) {
    if (fileName == null) {
      return null;
    }

    ServiceTags ret = null;

    try {
      File               file        = new File(fileName);
      PerfMemTimeTracker loadTracker = new PerfMemTimeTracker("Load tags");

      log("loading tags(file=" + fileName + ")");

      {
        PerfMemTimeTracker tracker = new PerfMemTimeTracker("Read tags");

        try (FileReader reader = new FileReader(file)) {
          ret = gson.fromJson(reader, ServiceTags.class);
        }

        tracker.stop();
        loadTracker.addChild(tracker);
      }

      if (deDup) {
        PerfMemTimeTracker tracker = new PerfMemTimeTracker("DeDupTags");

        int countOfDuplicateTags = ret.dedupTags();

        tracker.stop();
        loadTracker.addChild(tracker);
        log("DeDupTags(duplicateTags=" + countOfDuplicateTags + ")");
      }

      if (deDupStrings) {
        PerfMemTimeTracker tracker = new PerfMemTimeTracker("DeDupStrings");

        ret.dedupStrings();

        tracker.stop();
        loadTracker.addChild(tracker);
      }

      loadTracker.stop();
      parent.addChild(loadTracker);

      log("loaded tags(file=" + fileName + ", size=" + file.length() + "): " + toSummaryStr(ret));
    } catch (FileNotFoundException excp) {
      log(fileName + ": file does not exist!");
    } catch (IOException excp) {
      log(fileName, excp);
    }

    return ret;
  }

  private RangerRoles loadRoles(String fileName, PerfMemTimeTracker parent) {
    if (fileName == null) {
      return null;
    }

    RangerRoles ret = null;

    try {
      File               file        = new File(fileName);
      PerfMemTimeTracker loadTracker = new PerfMemTimeTracker("Load roles");

      log("loading roles(file=" + fileName + ")");

      try (FileReader reader = new FileReader(file)) {
        ret = gson.fromJson(reader, RangerRoles.class);
      }

      loadTracker.stop();
      parent.addChild(loadTracker);

      log("loaded roles(file=" + fileName + ", size=" + file.length() + "): " + toSummaryStr(ret));
    } catch (FileNotFoundException excp) {
      log(fileName + ": file does not exist!");
    } catch (IOException excp) {
      log(fileName, excp);
    }

    return ret;
  }

  private RangerUserStore loadUserStore(String fileName, PerfMemTimeTracker parent) {
    if (fileName == null) {
      return null;
    }

    RangerUserStore ret = null;

    try {
      File               file        = new File(fileName);
      PerfMemTimeTracker loadTracker = new PerfMemTimeTracker("Load userStore");

      log("loading userStore(file=" + fileName + ")");

      {
        PerfMemTimeTracker tracker = new PerfMemTimeTracker("Read userStore");

        try (FileReader reader = new FileReader(file)) {
          ret = gson.fromJson(reader, RangerUserStore.class);
        }

        tracker.stop();
        loadTracker.addChild(tracker);
      }

      if (deDupStrings) {
        PerfMemTimeTracker tracker = new PerfMemTimeTracker("DeDupStrings");

        ret.dedupStrings();

        tracker.stop();
        loadTracker.addChild(tracker);
      }

      loadTracker.stop();
      parent.addChild(loadTracker);

      log("loaded userStore(file=" + fileName + ", size=" + file.length() + "): " + toSummaryStr(ret) + ")");
    } catch (FileNotFoundException excp) {
        log(fileName + ": file does not exist!");
    } catch (IOException excp) {
      log(fileName, excp);
    }

    return ret;
  }

  private ServiceGdsInfo loadGdsInfo(String fileName, PerfMemTimeTracker parent) {
    if (fileName == null) {
      return null;
    }

    ServiceGdsInfo ret = null;

    try {
      File               file        = new File(fileName);
      PerfMemTimeTracker loadTracker = new PerfMemTimeTracker("Load gdsInfo");

      log("loading gdsInfo(file=" + fileName + ")");

      {
        PerfMemTimeTracker tracker = new PerfMemTimeTracker("Read gdsInfo");

        try (FileReader reader = new FileReader(file)) {
          ret = gson.fromJson(reader, ServiceGdsInfo.class);
        }

        tracker.stop();
        loadTracker.addChild(tracker);
      }

      if (deDupStrings) {
        PerfMemTimeTracker tracker = new PerfMemTimeTracker("DeDupStrings");

        ret.dedupStrings();

        tracker.stop();
        loadTracker.addChild(tracker);
      }

      loadTracker.stop();
      parent.addChild(loadTracker);

      log("loaded gdsInfo(file=" + fileName + ", size=" + file.length() + "): " + toSummaryStr(ret) + ")");
    } catch (FileNotFoundException excp) {
      log(fileName + ": file does not exist!");
    } catch (IOException excp) {
      log(fileName, excp);
    }

    return ret;
  }

  private RangerBasePlugin createRangerPlugin(ServicePolicies policies, ServiceTags tags, RangerRoles roles, RangerUserStore userStore, ServiceGdsInfo gdsInfo, PerfMemTimeTracker parent) {
    RangerBasePlugin ret = null;

    if (policies != null) {
      String             serviceType  = policies.getServiceDef().getName();
      String             serviceName  = policies.getServiceName();
      RangerPluginConfig pluginConfig = new RangerPluginConfig(serviceType, serviceName, serviceType, null, null, getPolicyEngineOptions());

      PerfMemTimeTracker tracker = new PerfMemTimeTracker("RangerBasePlugin initialization");

      log("Initializing RangerBasePlugin...");

      ret = new RangerBasePlugin(pluginConfig, policies, tags, roles, userStore, gdsInfo);

      tracker.stop();
      parent.addChild(tracker);
      log("Initialized RangerBasePlugin.");
    }

    return ret;
  }

  private static CommandLine parseArgs(String[] args) {
    Option help         = new Option("h", "help", false, "show help");
    Option deDup        = new Option("d", "deDup", true, "deDup string/tags");
    Option policies     = new Option("p", "policies", true, "policies file");
    Option tags         = new Option("t", "tags", true, "tags file");
    Option roles        = new Option("r", "roles", true, "roles file");
    Option userStore    = new Option("u", "userStore", true, "userStore file");
    Option optimizeMode = new Option("o", "optMode", true, "optimization mode: space|retrieval");

    Options options = new Options();

    options.addOption(help);
    options.addOption(policies);
    options.addOption(tags);
    options.addOption(roles);
    options.addOption(userStore);
    options.addOption(deDup);
    options.addOption(optimizeMode);

    try {
      CommandLine cmdLine = new DefaultParser().parse(options, args);

      if (! cmdLine.hasOption("h")) {
        return cmdLine;
      }

      new HelpFormatter().printHelp("RangerMemSizing", options);
    } catch (ParseException excp) {
      System.out.println("Failed to parse arguments");
      excp.printStackTrace(System.out);
    }

    return null;
  }

  private Gson createGson() {
    Gson gson = null;

    try {
      gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").create();
    } catch(Throwable excp) {
      log("failed to create GsonBuilder object", excp);
    }

    return gson;
  }

  private void log(String msg) {
    out.println(DATE_FORMAT.format(new Date()) + ": " +msg);
  }

  private void log(String msg, Throwable excp) {
    out.println(DATE_FORMAT.format(new Date()) + ": " +msg);

    excp.printStackTrace(out);
  }

  private RangerPolicyEngineOptions getPolicyEngineOptions() {
    RangerPolicyEngineOptions ret = new RangerPolicyEngineOptions();

    ret.disablePolicyRefresher      = true;
    ret.disableTagRetriever         = true;
    ret.disableUserStoreRetriever   = true;
    ret.optimizeTrieForSpace        = optimizationMode.equals(OPT_MODE_SPACE);
    ret.optimizeTrieForRetrieval    = !ret.optimizeTrieForSpace;
    ret.optimizeTagTrieForSpace     = ret.optimizeTrieForSpace;
    ret.optimizeTagTrieForRetrieval = ret.optimizeTrieForRetrieval;

    return ret;
  }

  private static String toSummaryStr(ServicePolicies policies) {
    int policyCount = 0;

    if (policies != null) {
      if (policies.getPolicies() != null) {
        policyCount += policies.getPolicies().size();
      }

      if (policies.getTagPolicies() != null && policies.getTagPolicies().getPolicies() != null) {
        policyCount += policies.getTagPolicies().getPolicies().size();
      }

      if (policies.getSecurityZones() != null) {
        for (SecurityZoneInfo zoneInfo : policies.getSecurityZones().values()) {
          if (zoneInfo.getPolicies() != null) {
            policyCount += zoneInfo.getPolicies().size();
          }
        }
      }
    }

    return "policyCount=" + policyCount;
  }

  private static String toSummaryStr(ServiceTags tags) {
    int tagDefCount = 0;
    int tagCount    = 0;
    int resourceCount = 0;

    if (tags != null) {
      if (tags.getTagDefinitions() != null) {
        tagDefCount = tags.getTagDefinitions().size();
      }

      if (tags.getTags() != null) {
        tagCount = tags.getTags().size();
      }

      if (tags.getServiceResources() != null) {
        resourceCount = tags.getServiceResources().size();
      }
    }

    return "tagDefCount=" + tagDefCount + ", tagCount" + tagCount + ", resourceCount=" + resourceCount;
  }

  private static String toSummaryStr(RangerRoles roles) {
    int roleCount = 0;

    if (roles != null) {
      if (roles.getRangerRoles() != null) {
        roleCount = roles.getRangerRoles().size();
      }
    }

    return "roleCount=" + roleCount;
  }

  private static String toSummaryStr(RangerUserStore userStore) {
    int userCount      = 0;
    int groupCount     = 0;
    int userGroupCount = 0;

    if (userStore != null) {
      if (userStore.getUserAttrMapping() != null) {
        userCount = userStore.getUserAttrMapping().size();
      }

      if (userStore.getGroupAttrMapping() != null) {
        groupCount = userStore.getGroupAttrMapping().size();
      }

      if (userStore.getUserGroupMapping() != null) {
        for (Set<String> userGroups : userStore.getUserGroupMapping().values()) {
          userGroupCount += userGroups.size();
        }
      }
    }

    return "users=" + userCount + ", groups=" + groupCount + ", userGroupMappings=" + userGroupCount;
  }

  private static String toSummaryStr(ServiceGdsInfo gdsInfo) {
    int dataShareCount = 0;
    int resourcesCount = 0;
    int datasetCount   = 0;
    int projectCount   = 0;

    if (gdsInfo != null) {
      if (gdsInfo.getDataShares() != null) {
        dataShareCount = gdsInfo.getDataShares().size();
      }

      if (gdsInfo.getResources() != null) {
        resourcesCount = gdsInfo.getResources().size();
      }

      if (gdsInfo.getDatasets() != null) {
        datasetCount = gdsInfo.getDatasets().size();
      }

      if (gdsInfo.getProjects() != null) {
        projectCount = gdsInfo.getProjects().size();
      }
    }

    return "dataShares=" + dataShareCount + ", resources=" + resourcesCount + ", datasets=" + datasetCount + ", projects=" + projectCount;
  }
}
