/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graphdb.janus.dynamo;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.TimeUnit;

class DynamoTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoTestUtils.class);

  private static final String BASE_CONTAINER_NAME = "atlas-dynamo-test-";
  private static final String IMAGE_NAME = "amazon/dynamodb-local";
  private static final String PORT_MAPPING = "8000:8000";


  static void setFakeCredentials() {
    LOG.debug("Setting to use janus");
    System.setProperty("aws.accessKeyId", "abc");
    System.setProperty("aws.secretKey", "123");
    System.setProperty("janus.use.dynamo", "true");
  }

  static String startDockerDynamo() throws IOException, InterruptedException {
    String name = genContainerName();
    LOG.info("Starting DynamoDB in a container with name " + name);
    if (runCmdAndPrintStreams(new String[] {"docker", "run", "--name", name, "-p", PORT_MAPPING, "-d",
        IMAGE_NAME}, 300) != 0) {
      throw new IOException("Failed to run docker image");
    }
    return name;
  }

  static void shutdownDockerDynamo(String name) throws IOException, InterruptedException {
    if (name == null) return;
    LOG.info("Shutting down DynamoDB in docker container");
    if (runCmdAndPrintStreams(new String[]{"docker", "stop", name}, 30) != 0) {
      throw new IOException("Failed to stop docker container");
    }
    runCmdAndPrintStreams(new String[] {"docker", "rm", name}, 30);
  }

  private static int runCmdAndPrintStreams(String[] cmd, long secondsToWait) throws InterruptedException, IOException {
    ProcessResults results = runCmd(cmd, secondsToWait);
    if (results.rc != 0) {
      LOG.error("Failed to run command " + StringUtils.join(cmd));
      LOG.error("stdout <" + results.stdout + ">");
      LOG.error("stderr <" + results.stderr + ">");
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Stdout from proc: " + results.stdout);
      LOG.debug("Stderr from proc: " + results.stderr);
    }
    return results.rc;
  }

  private static ProcessResults runCmd(String[] cmd, long secondsToWait) throws IOException, InterruptedException {
    LOG.debug("Going to run: " + StringUtils.join(cmd, " "));
    Process proc = Runtime.getRuntime().exec(cmd);
    if (!proc.waitFor(secondsToWait, TimeUnit.SECONDS)) {
      throw new RuntimeException("Process " + cmd[0] + " failed to run in " + secondsToWait +
          " seconds");
    }
    BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    final StringBuilder lines = new StringBuilder();
    reader.lines()
        .forEach(s -> lines.append(s).append('\n'));

    reader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
    final StringBuilder errLines = new StringBuilder();
    reader.lines()
        .forEach(s -> errLines.append(s).append('\n'));
    return new ProcessResults(lines.toString(), errLines.toString(), proc.exitValue());
  }

  private static String genContainerName() {
    return BASE_CONTAINER_NAME + Math.abs(new Random().nextInt());
  }

  private static class ProcessResults {
    final String stdout;
    final String stderr;
    final int rc;

    ProcessResults(String stdout, String stderr, int rc) {
      this.stdout = stdout;
      this.stderr = stderr;
      this.rc = rc;
    }
  }
}
