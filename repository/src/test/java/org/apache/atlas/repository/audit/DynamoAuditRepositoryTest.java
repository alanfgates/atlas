/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.audit;

import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.dynamo.DynamoTestUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;

import java.io.IOException;
import java.util.List;

public class DynamoAuditRepositoryTest extends AuditRepositoryTestBase {
  private static String containerName;

  @BeforeClass
  public void startDynamo() throws IOException, InterruptedException, AtlasException {
    DynamoTestUtils.setFakeCredentials();
    containerName = DynamoTestUtils.startDockerDynamo();
    DynamoBasedAuditRepository repo = new DynamoBasedAuditRepository();
    repo.start();
    eventRepository = repo;
  }

  @AfterClass
  public void stopDynamo() throws IOException, InterruptedException {
    DynamoTestUtils.shutdownDockerDynamo(containerName);
  }

  @BeforeTest
  @Override
  public void setUp() {
  }
}
