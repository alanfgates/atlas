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

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Singleton;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Dynamo based audit repository.  This is built to be thread safe, meaning that multiple threads can list and put
 * simultaneously.  It does assume that start is only called once and that it is called and finishes before
 * any put or list calls are made.
 */
@Singleton
@Component
@ConditionalOnAtlasProperty(property = "atlas.EntityAuditRepository.impl")
public class DynamoBasedAuditRepository extends AbstractStorageBasedAuditRepository {

  private static final String GRAPH_PREFIX = "atlas.graph";
  private static final String DYNAMO_PREFIX = "storage.dynamo.";
  private static final String DYNAMO_URL = DYNAMO_PREFIX + "url";
  private static final String DYNAMO_REGION = DYNAMO_PREFIX + "region";
  private static final String DYNAMO_READ_THROUGHPUT = DYNAMO_PREFIX + "readthroughput";
  private static final String DYNAMO_WRITE_THROUGHPUT = DYNAMO_PREFIX + "writethroughput";
  private static final String DYNAMO_CONFIG_TABLE_NAME = HBaseBasedAuditRepository.CONFIG_PREFIX + ".dynamo.tablename";
  private static final String PARTITION_KEY = "entityId";
  private static final String SORT_KEY = "timestamp";
  private static final String COLUMN_ACTION = "action";
  private static final String COLUMN_USER = "user";
  private static final String COLUMN_DETAILS = "details";
  private static final String COLUMN_DEFINITION = "definition";

  private static final Logger LOG = LoggerFactory.getLogger(DynamoBasedAuditRepository.class);

  private String tableName;
  private Configuration conf;
  private boolean persistEntityDefinition;

  @Override
  public void putEventsV1(List<EntityAuditEvent> events) throws AtlasException {
    try {
      putEvents(EventV1Wrapper.convertToWrapper(events));
    } catch (AtlasBaseException e) {
      throw new AtlasException(e);
    }
  }

  @Override
  public List<EntityAuditEvent> listEventsV1(String entityId, String startKey, short n) throws AtlasException {
    try {
      return EventV1Wrapper.convertFromWrapper(listEvents(1, entityId, startKey, n));
    } catch (AtlasBaseException e) {
      throw new AtlasException(e);
    }
  }

  @Override
  public void putEventsV2(List<EntityAuditEventV2> events) throws AtlasBaseException {
    putEvents(EventV2Wrapper.convertToWrapper(events));
  }

  @Override
  public List<EntityAuditEventV2> listEventsV2(String entityId, String startKey, short n) throws AtlasBaseException {
    return EventV2Wrapper.convertFromWrapper(listEvents(2, entityId, startKey, n));
  }

  @Override
  public Set<String> getEntitiesWithTagChanges(long fromTimestamp, long toTimestamp) throws AtlasBaseException {
    try (DynamoConnection dynamo = getConnection()) {
      Table table = getTable(dynamo);
      ItemCollection<ScanOutcome> results =
          table.scan(new ScanFilter(SORT_KEY).between(convertTimestamp(toTimestamp), convertTimestamp(fromTimestamp)));
      Set<String> eventIds = new HashSet<>();
      for (Item item : results) {
        eventIds.add(item.getString(PARTITION_KEY));
      }
      return eventIds;
    } catch (Exception e) {
      LOG.error("Failed to scan", e);
      throw new AtlasBaseException("Failed to scan", e);
    }
  }

  @Override
  public void start() throws AtlasException {
    Configuration fullConf = ApplicationProperties.get();
    conf = ApplicationProperties.getSubsetConfiguration(fullConf, GRAPH_PREFIX);
    assert fullConf.getString(DYNAMO_CONFIG_TABLE_NAME) != null;
    tableName = fullConf.getString(DYNAMO_CONFIG_TABLE_NAME);
    persistEntityDefinition = fullConf.getBoolean(CONFIG_PERSIST_ENTITY_DEFINITION, false);

    // Create the table if it does not already exist
    try (DynamoConnection dynamo = getConnection()) {
      LOG.info("Looking for table " + tableName);
      TableCollection<ListTablesResult> existingTables = dynamo.get().listTables();
      boolean foundIt = false;
      for (Table maybe : existingTables) {
        if (maybe.getTableName().equals(tableName)) {
          foundIt = true;
          break;
        }
      }
      if (!foundIt) {
        LOG.info("Table " + tableName + " not found, will create it");
        CreateTableRequest tableDef = new CreateTableRequest()
            .withTableName(tableName)
            .withKeySchema(
                new KeySchemaElement(PARTITION_KEY, KeyType.HASH),
                new KeySchemaElement(SORT_KEY, KeyType.RANGE))
            .withAttributeDefinitions(
                new AttributeDefinition(PARTITION_KEY, ScalarAttributeType.S),
                new AttributeDefinition(SORT_KEY, ScalarAttributeType.N))
            .withProvisionedThroughput(new ProvisionedThroughput(conf.getLong(DYNAMO_READ_THROUGHPUT, 10L), conf.getLong(DYNAMO_WRITE_THROUGHPUT, 10L)));
        try {
          Table table = dynamo.get().createTable(tableDef);
          table.waitForActive();
        } catch (Exception e) {
          throw new AtlasException("Unable to create table " + tableName, e);
        }
      }
    }
  }

  @Override
  public void stop() {

  }

  private DynamoConnection getConnection() {
    LOG.debug("Connecting to DynamoDB at endpoint " + conf.getString(DYNAMO_URL) + " and region " +
        conf.getString(DYNAMO_REGION));
    assert conf.getString(DYNAMO_URL) != null;
    assert conf.getString(DYNAMO_REGION) != null;
    AmazonDynamoDB client = AmazonDynamoDBClientBuilder
        .standard()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(conf.getString(DYNAMO_URL), conf.getString(DYNAMO_REGION))
        )
        .build();
    return new DynamoConnection(new DynamoDB(client));
  }

  // The interface wants timestamps to come back in reverse order.  By start it also means closest to now, not
  // earliest in time.  So multiple by -1 to reverse the sort order.
  private long convertTimestamp(long timestamp) {
    return timestamp * -1;
  }

  private void putEvents(List<EventWrapper> events) throws AtlasBaseException {
    try (DynamoConnection dynamo = getConnection()) {
      Table table = getTable(dynamo);
      for (EventWrapper event : events) {
        Item item = new Item()
            .withPrimaryKey(PARTITION_KEY, event.getEntityId(), SORT_KEY, convertTimestamp(event.getTimestamp()))
            .withString(COLUMN_ACTION, event.getAction())
            .withString(COLUMN_USER, event.getUser())
            .withString(COLUMN_DETAILS, event.getDetails());
        if (persistEntityDefinition) {
          item.withString(COLUMN_DEFINITION, event.getDefinition());
        }
        LOG.debug("Inserting event " + event.getEntityId() + ":" + event.getTimestamp());
        table.putItem(item);
      }
    } catch (Exception e) {
      LOG.error("Failed to insert item", e);
      throw new AtlasBaseException("Failed to insert item", e);
    }
  }

  private List<EventWrapper> listEvents(int version, String entityId, String startKey, short n) throws AtlasBaseException {
    // How this works, as near as I can tell.  The startKey is not the timestamp, as one might reasonably expect.
    // Instead, it is the entityId:timestamp.  Furthermore, it is expected that this method return the entries in
    // reverse order (that is, more recent entries first).
    try (DynamoConnection dynamo = getConnection()) {
      Table table = getTable(dynamo);

      ItemCollection<QueryOutcome> results;
      if (StringUtils.isEmpty(startKey)) {
        LOG.debug("Looking for all events with entityId " + entityId);
        results = table.query(PARTITION_KEY, entityId);
      } else {
        long start = convertTimestamp(Long.valueOf(startKey.split(FIELD_SEPARATOR)[1]));
        LOG.debug("Looking for all events with entityId " + entityId + " and timestamp at or before " + (start * -1));
        results = table.query(PARTITION_KEY, entityId, new RangeKeyCondition(SORT_KEY).ge(start));
      }

      List<EventWrapper> events = new ArrayList<>();
      for (Item item : results) {
        if (events.size() >= n) break;
        EventWrapper event = version == 1 ? new EventV1Wrapper(new EntityAuditEvent()) : new EventV2Wrapper(new EntityAuditEventV2());
        event.setEntityId(entityId);
        event.setTimestamp(convertTimestamp(item.getLong(SORT_KEY)));
        event.setEventKey();
        event.setAction(item.getString(COLUMN_ACTION));
        event.setUser(item.getString(COLUMN_USER));
        event.setDetails(item.getString(COLUMN_DETAILS));
        if (persistEntityDefinition) {
          event.setDefinition(item.getString(COLUMN_DEFINITION));
        }
        events.add(event);
      }
      return events;
    } catch (Exception e) {
      LOG.error("Failed to query", e);
      throw new AtlasBaseException("Failed to query", e);
    }
  }

  private Table getTable(DynamoConnection dynamo) {
    return dynamo.get().getTable(tableName);
  }

  private class DynamoConnection implements Closeable {
    private final DynamoDB dynamo;

    DynamoConnection(DynamoDB dynamo) {
      this.dynamo = dynamo;
    }

    DynamoDB get() {
      return dynamo;
    }

    @Override
    public void close() {
      dynamo.shutdown();
    }
  }

  // Why does this need to be done here?  A generic interface should be included somewhere higher up.
  private interface EventWrapper {
    String getEntityId();
    long getTimestamp();
    String getAction();
    String getUser();
    String getDetails();
    String getDefinition();
    void setEntityId(String id);
    void setTimestamp(long ts);
    void setAction(String action);
    void setUser(String user);
    void setDetails(String details);
    void setDefinition(String def);
    void setEventKey();
  }

  private static class EventV1Wrapper implements EventWrapper {
    private final EntityAuditEvent event;

    EventV1Wrapper(EntityAuditEvent event) {
      this.event = event;
    }

    @Override
    public String getEntityId() {
      return event.getEntityId();
    }

    @Override
    public long getTimestamp() {
      return event.getTimestamp();
    }

    @Override
    public String getAction() {
      return event.getAction().name();
    }

    @Override
    public String getUser() {
      return event.getUser();
    }

    @Override
    public String getDetails() {
      return event.getDetails();
    }

    @Override
    public String getDefinition() {
      return event.getEntityDefinitionString();
    }

    @Override
    public void setEntityId(String id) {
      event.setEntityId(id);
    }

    @Override
    public void setTimestamp(long ts) {
      event.setTimestamp(ts);
    }

    @Override
    public void setAction(String action) {
      event.setAction(EntityAuditEvent.EntityAuditAction.valueOf(action));
    }

    @Override
    public void setUser(String user) {
      event.setUser(user);

    }

    @Override
    public void setDetails(String details) {
      event.setDetails(details);
    }

    @Override
    public void setDefinition(String def) {
      event.setEntityDefinition(def);
    }

    @Override
    public void setEventKey() {
      event.setEventKey(getEntityId() + FIELD_SEPARATOR + getTimestamp());
    }

    private EntityAuditEvent get() {
      return event;
    }

    static List<EventWrapper> convertToWrapper(List<EntityAuditEvent> events) {
      return events
          .stream()
          .map((Function<EntityAuditEvent, EventWrapper>) EventV1Wrapper::new)
          .collect(Collectors.toList());
    }

    static List<EntityAuditEvent> convertFromWrapper(List<EventWrapper> events) {
      return events
          .stream()
          .map(eventWrapper -> ((EventV1Wrapper)eventWrapper).get())
          .collect(Collectors.toList());
    }
  }

  private static class EventV2Wrapper implements EventWrapper {
    private final EntityAuditEventV2 event;

    EventV2Wrapper(EntityAuditEventV2 event) {
      this.event = event;
    }

    @Override
    public String getEntityId() {
      return event.getEntityId();
    }

    @Override
    public long getTimestamp() {
      return event.getTimestamp();
    }

    @Override
    public String getAction() {
      return event.getAction().name();
    }

    @Override
    public String getUser() {
      return event.getUser();
    }

    @Override
    public String getDetails() {
      return event.getDetails();
    }

    @Override
    public String getDefinition() {
      return event.getEntityDefinitionString();
    }

    private EntityAuditEventV2 get() {
      return event;
    }

    @Override
    public void setEntityId(String id) {
      event.setEntityId(id);
    }

    @Override
    public void setTimestamp(long ts) {
      event.setTimestamp(ts);
    }

    @Override
    public void setAction(String action) {
      event.setAction(EntityAuditEventV2.EntityAuditActionV2.valueOf(action));
    }

    @Override
    public void setUser(String user) {
      event.setUser(user);

    }

    @Override
    public void setDetails(String details) {
      event.setDetails(details);
    }

    @Override
    public void setDefinition(String def) {
      event.setEntityDefinition(def);
    }

    @Override
    public void setEventKey() {
      event.setEventKey(getEntityId() + FIELD_SEPARATOR + getTimestamp());
    }

    static List<EventWrapper> convertToWrapper(List<EntityAuditEventV2> events) {
      return events
          .stream()
          .map((Function<EntityAuditEventV2, EventWrapper>) EventV2Wrapper::new)
          .collect(Collectors.toList());
    }

    static List<EntityAuditEventV2> convertFromWrapper(List<EventWrapper> events) {
      return events
          .stream()
          .map(eventWrapper -> ((EventV2Wrapper)eventWrapper).get())
          .collect(Collectors.toList());
    }
  }
}
