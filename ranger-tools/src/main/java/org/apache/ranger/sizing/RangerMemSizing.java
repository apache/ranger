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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServicePolicies.SecurityZoneInfo;
import org.apache.ranger.plugin.util.ServiceTags;


public class RangerMemSizing {
  private static final String OPT_MODE_SPACE      = "space";
  private static final String OPT_MODEL_RETRIEVAL = "retrieval";

  private final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

  private static final TypeReference<List<RangerAccessRequest>> TYPE_LIST_REQUESTS = new TypeReference<List<RangerAccessRequest>>() {};

  private final String      policyFile;
  private final String      tagFile;
  private final String      rolesFile;
  private final String      userStoreFile;
  private final String      genRequestsFile;
  private final Set<String> genResourceKeys;
  private final String      evalRequestsFile;
  private final int         evalClientsCount;
  private final boolean     deDup;
  private final boolean     deDupStrings;
  private final String      optimizationMode;
  private final boolean     reuseResourceMatchers;
  private final PrintStream out;

  public RangerMemSizing(CommandLine cmdLine) {
    this.out              = System.out;
    this.policyFile       = cmdLine.getOptionValue('p');
    this.tagFile          = cmdLine.getOptionValue('t');
    this.rolesFile        = cmdLine.getOptionValue('r');
    this.userStoreFile    = cmdLine.getOptionValue('u');
    this.genRequestsFile  = cmdLine.getOptionValue('q');
    this.genResourceKeys  = csvToSet(cmdLine.getOptionValue('k'));
    this.evalRequestsFile = cmdLine.getOptionValue('e');
    this.evalClientsCount = cmdLine.hasOption('c') ? Integer.parseInt(cmdLine.getOptionValue('c')) : 1;
    this.deDup            = Boolean.parseBoolean(cmdLine.getOptionValue("d", "true"));
    this.deDupStrings     = this.deDup;
    this.optimizationMode = StringUtils.startsWithIgnoreCase(cmdLine.getOptionValue('o', "space"), "s") ? OPT_MODE_SPACE : OPT_MODEL_RETRIEVAL;
    this.reuseResourceMatchers = Boolean.parseBoolean(cmdLine.getOptionValue('m', "true"));
  }

  public void run() {
    PerfMemTimeTracker tracker = new PerfMemTimeTracker("RangerMemSizing");

    ServicePolicies  policies  = loadPolicies(policyFile, tracker);
    ServiceTags      tags      = loadTags(tagFile, tracker);
    RangerRoles      roles     = loadRoles(rolesFile, tracker);
    RangerUserStore  userStore = loadUserStore(userStoreFile, tracker);
    RangerBasePlugin plugin    = createRangerPlugin(policies, tags, roles, userStore, tracker);
    int              genReqCount   = 0;
    int              evalReqCount  = 0;
    int              evalAvgTimeNs = 0;

    if (StringUtils.isNotBlank(genRequestsFile)) {
      genReqCount = generateRequestsFile(policies, tags, genRequestsFile, tracker);
    }

    if (StringUtils.isNotBlank(evalRequestsFile)) {
      int[] res = evaluateRequests(evalRequestsFile, plugin, tracker);

      evalReqCount  = res[0];
      evalAvgTimeNs = res[1];
    }

    tracker.stop();

    out.println();
    out.println("Parameters:");
    if (policies != null) {
      out.println("  Policies:      file=" + policyFile + ", size=" + new File(policyFile).length() + ", " + toSummaryStr(policies));
    }

    if (tags != null) {
      out.println("  Tags:          file=" + tagFile + ", size=" + new File(tagFile).length() + ", " + toSummaryStr(tags));
    }

    if (roles != null) {
      out.println("  Roles:         file=" + rolesFile + ", size=" + new File(rolesFile).length() + ", " + toSummaryStr(roles));
    }

    if (userStore != null) {
      out.println("  UserStore:     file=" + userStoreFile + ", size=" + new File(userStoreFile).length() + ", " + toSummaryStr(userStore));
    }

    if (genRequestsFile != null) {
      out.println("  GenReq:        file=" + genRequestsFile + ", requestCount=" + genReqCount);
    }

    if (evalRequestsFile != null) {
      out.println("  EvalReq:       file=" + evalRequestsFile + ", requestCount=" + evalReqCount + ", avgTimeTaken=" + evalAvgTimeNs +  "ns, clientCount=" + evalClientsCount);
    }

    out.println("  DeDup:         " + deDup);
    out.println("  OptMode:       " + optimizationMode);
    out.println("  ReuseMatchers: " + reuseResourceMatchers);
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
          ret = JsonUtils.jsonToObject(reader, ServicePolicies.class);
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
          ret = JsonUtils.jsonToObject(reader, ServiceTags.class);
        }

        tracker.stop();

        loadTracker.addChild(tracker);
      }

      if (deDup) {
        PerfMemTimeTracker tracker = new PerfMemTimeTracker("DeDupTags");

        int countOfDuplicateTags = ret.dedupTags();

        tracker.stop();
        loadTracker.addChild(tracker);

        log("DeDupTags(): duplicateTagsCount=" + countOfDuplicateTags);
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
        ret = JsonUtils.jsonToObject(reader, RangerRoles.class);
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
          ret = JsonUtils.jsonToObject(reader, RangerUserStore.class);
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

  private RangerBasePlugin createRangerPlugin(ServicePolicies policies, ServiceTags tags, RangerRoles roles, RangerUserStore userStore, PerfMemTimeTracker parent) {
    RangerBasePlugin ret = null;

    if (policies != null) {
      String             serviceType  = policies.getServiceDef().getName();
      String             serviceName  = policies.getServiceName();
      RangerPluginConfig pluginConfig = new RangerPluginConfig(serviceType, serviceName, serviceType, null, null, getPolicyEngineOptions());

      PerfMemTimeTracker tracker = new PerfMemTimeTracker("RangerBasePlugin initialization");

      log("Initializing RangerBasePlugin...");

      ret = new RangerBasePlugin(pluginConfig, policies, tags, roles, userStore);

      tracker.stop();
      parent.addChild(tracker);
      log("Initialized RangerBasePlugin.");
    }

    return ret;
  }

  private int generateRequestsFile(ServicePolicies policies, ServiceTags tags, String fileName, PerfMemTimeTracker parent) {
    ObjectMapper mapper = JsonUtils.getMapper();

    initMapper(mapper);

    PerfMemTimeTracker tracker = new PerfMemTimeTracker("generateRequests");

    Collection<RangerAccessRequest> requests = new PerfRequestGenerator(genResourceKeys).generate(policies, tags);

    log("generateRequestsFile(): saving " + requests.size() + " requests..");

    try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName))) {
      writer.write('[');

      boolean isFirst = true;

      for (RangerAccessRequest request : requests) {
        if (!isFirst) {
          writer.write(',');
        } else {
          isFirst = false;
        }
        writer.newLine();

        writer.write(mapper.writeValueAsString(request));
      }

      writer.newLine();
      writer.write(']');

      log("generateRequestsFile(): saved " + requests.size() + " requests");
    } catch (IOException excp) {
      log("generateRequestsFile(): failed", excp);
    }

    tracker.stop();
    parent.addChild(tracker);

    return requests.size();
  }

  private int[] evaluateRequests(String evalRequestsFile, RangerBasePlugin plugin, PerfMemTimeTracker parent) {
    PerfMemTimeTracker tracker = new PerfMemTimeTracker("evaluateRequests");

    final List<RangerAccessRequest> requests          = readRequests(evalRequestsFile, tracker);
    final Thread[]                  clients           = new Thread[evalClientsCount];
    final AtomicInteger             idxNextRequest    = new AtomicInteger();
    final AtomicLong                totalTimeTakenNs  = new AtomicLong();

    for (int idxClient = 0; idxClient < evalClientsCount; idxClient++) {
      clients[idxClient] = new Thread(() -> {
        do {
          int idxReq = idxNextRequest.getAndIncrement();

          if (idxReq >= requests.size()) {
            break;
          }

          RangerAccessRequest request = requests.get(idxReq);

          long startTime = System.nanoTime();

          RangerAccessResult result = plugin.isAccessAllowed(request);

          totalTimeTakenNs.getAndAdd(System.nanoTime() - startTime);

          if ((idxReq + 1) % 1000 == 0) {
            log("  evaluated requests: " + (idxReq + 1) + ", avgTimeTaken=" + (totalTimeTakenNs.get() / idxReq) + "ns");
          }

          requests.set(idxReq, null); // so that objects associated with the request can be freed
        } while (true);
      });
    }

    log("evaluateRequests(): evaluating " + requests.size() + " requests in " + clients.length + " clients...");

    for (Thread client : clients) {
      client.run();
    }

    log("evaluateRequests(): waiting for " + clients.length + " clients to complete...");
    for (Thread client : clients) {
      try {
        client.join();
      } catch (Exception excp) {
        // ignore
      }
    }

    tracker.stop();
    parent.addChild(tracker);

    int avgTimeTakenNs = (int) (totalTimeTakenNs.get() / idxNextRequest.get());

    log("evaluateRequests(): evaluated " + idxNextRequest.get() + " requests, avgTimeTaken=" + avgTimeTakenNs + "ns");

    return new int[] { requests.size(), avgTimeTakenNs };
  }

  private List<RangerAccessRequest> readRequests(String fileName, PerfMemTimeTracker parent) {
    PerfMemTimeTracker tracker = new PerfMemTimeTracker("readRequests");

    List<RangerAccessRequest> ret = null;

    ObjectMapper mapper = JsonUtils.getMapper();

    initMapper(mapper);

    try (InputStream inStr = Files.newInputStream(Paths.get(evalRequestsFile))) {
      ret = mapper.readValue(inStr, TYPE_LIST_REQUESTS);
    } catch (IOException excp) {
      log("readRequests(): failed to read file " + evalRequestsFile, excp);
    }

    if (ret == null) {
      ret = Collections.emptyList();
    }

    tracker.stop();
    parent.addChild(tracker);

    log("readRequests(file=" + fileName + ", size=" + new File(evalRequestsFile).length() + "): request count=" + ret.size());

    return ret;
  }

  private static CommandLine parseArgs(String[] args) {
    Option help         = new Option("h", "help", false, "show help");
    Option deDup        = new Option("d", "deDup", true, "deDup string, tags: true|false");
    Option policies     = new Option("p", "policies", true, "policies file");
    Option tags         = new Option("t", "tags", true, "tags file");
    Option roles        = new Option("r", "roles", true, "roles file");
    Option userStore    = new Option("u", "userStore", true, "userStore file");
    Option genRequests  = new Option("q", "genRequests", true, "generate requests file");
    Option evalRequests = new Option("e", "evalRequests", true, "eval requests file");
    Option evalClients  = new Option("c", "evalClients", true, "eval clients count");
    Option gdsInfo      = new Option("g", "gdsInfo", true, "gdsInfo file");
    Option optimizeMode = new Option("o", "optMode", true, "optimization mode: space|retrieval");
    Option reuseResourceMatchers = new Option("m", "reuseResourceMatchers", true, "reuse resource matchers: true|false");
    Option genResourceKeys       = new Option("k", "genResourceKeys", true, "list of resourceKeys (comma separated) to generate requests for");

    Options options = new Options();

    options.addOption(help);
    options.addOption(policies);
    options.addOption(tags);
    options.addOption(roles);
    options.addOption(userStore);
    options.addOption(genRequests);
    options.addOption(evalRequests);
    options.addOption(evalClients);
    options.addOption(gdsInfo);
    options.addOption(deDup);
    options.addOption(optimizeMode);
    options.addOption(reuseResourceMatchers);
    options.addOption(genResourceKeys);

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
    ret.enableResourceMatcherReuse  = reuseResourceMatchers;

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

    return "tagDefCount=" + tagDefCount + ", tagCount=" + tagCount + ", resourceCount=" + resourceCount;
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

  private Set<String> csvToSet(String str) {
    return StringUtils.isBlank(str) ? Collections.emptySet() : new HashSet<>(Arrays.asList(StringUtils.split(str, ',')));
  }

  private void initMapper(ObjectMapper mapper) {
    SimpleModule serDeModule = new SimpleModule("RangerMemSizing", new Version(1, 0, 0, null, null, null));

    serDeModule.addSerializer(RangerAccessResource.class, new RangerAccessResourceSerializer());
    serDeModule.addSerializer(RangerAccessResourceImpl.class, new RangerAccessResourceImplSerializer());
    serDeModule.addDeserializer(RangerAccessResource.class, new RangerAccessResourceDeserializer());

    serDeModule.addSerializer(RangerAccessRequest.class, new RangerAccessRequestSerializer());
    serDeModule.addSerializer(RangerAccessRequestImpl.class, new RangerAccessRequestImplSerializer());
    serDeModule.addDeserializer(RangerAccessRequest.class, new RangerAccessRequestDeserializer());

    mapper.registerModule(serDeModule);
  }


  static class TestRangerAccessResourceImpl {
    private Map<String, Object> elements;

    public TestRangerAccessResourceImpl() {
    }

    public TestRangerAccessResourceImpl(Map<String, Object> elements) {
      this.elements = elements;
    }

    public Map<String, Object> getElements() { return elements; }
  }

  static class TestRangerAccessRequestImpl {
    private TestRangerAccessResourceImpl resource;
    private String                       accessType;
    private String                       user;
    private Set<String>                  userGroups;

    public TestRangerAccessRequestImpl() {
    }

    public TestRangerAccessRequestImpl(RangerAccessResource resource, String accessType, String user, Set<String> userGroups) {
      this.resource   = new TestRangerAccessResourceImpl(resource.getAsMap());
      this.accessType = accessType;
      this.user       = user;
      this.userGroups = userGroups;
    }

    public TestRangerAccessResourceImpl getResource() {
      return resource;
    }

    public String getAccessType() {
      return accessType;
    }

    public String getUser() {
      return user;
    }

    public Set<String> getUserGroups() {
      return userGroups;
    }

  }

  static class RangerAccessResourceSerializer extends JsonSerializer<RangerAccessResource> {
    @Override
    public void serialize(RangerAccessResource value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      JsonUtils.getMapper().writeValue(gen, new TestRangerAccessResourceImpl(value.getAsMap()));
    }
  }

  static class RangerAccessResourceImplSerializer extends JsonSerializer<RangerAccessResource> {
    @Override
    public void serialize(RangerAccessResource value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      JsonUtils.getMapper().writeValue(gen, new TestRangerAccessResourceImpl(value.getAsMap()));
    }
  }

  static class RangerAccessResourceDeserializer extends JsonDeserializer<RangerAccessResource> {
    @Override
    public RangerAccessResource deserialize(JsonParser parser, DeserializationContext context) throws IOException {
      TestRangerAccessResourceImpl resource = context.readValue(parser, TestRangerAccessResourceImpl.class);

      return new RangerAccessResourceImpl(resource.getElements());
    }
  }

  static class RangerAccessRequestSerializer extends JsonSerializer<RangerAccessRequest> {
    @Override
    public void serialize(RangerAccessRequest value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      JsonUtils.getMapper().writeValue(gen, new TestRangerAccessRequestImpl(value.getResource(), value.getAccessType(), value.getUser(), value.getUserGroups()));
    }
  }

  static class RangerAccessRequestImplSerializer extends JsonSerializer<RangerAccessRequestImpl> {
    @Override
    public void serialize(RangerAccessRequestImpl value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      JsonUtils.getMapper().writeValue(gen, new TestRangerAccessRequestImpl(value.getResource(), value.getAccessType(), value.getUser(), value.getUserGroups()));
    }
  }

  static class RangerAccessRequestDeserializer extends JsonDeserializer<RangerAccessRequest> {
    @Override
    public RangerAccessRequest deserialize(JsonParser parser, DeserializationContext context) throws IOException {
      TestRangerAccessRequestImpl req = context.readValue(parser, TestRangerAccessRequestImpl.class);

      return new RangerAccessRequestImpl(new RangerAccessResourceImpl(req.resource.getElements()), req.accessType, req.user, req.userGroups, null);
    }
  }
}
