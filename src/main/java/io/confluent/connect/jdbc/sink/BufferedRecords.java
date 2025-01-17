/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaBuilder;
//import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Field;




import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
//import java.util.GregorianCalendar;
//import java.util.TimeZone;
//import java.util.Calendar;


import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.INSERT;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

  private final TableId tableId;
  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new ArrayList<>();
  private Schema keySchema;
  private Schema valueSchema;
  private RecordValidator recordValidator;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement updatePreparedStatement;
  private PreparedStatement deletePreparedStatement;
  private StatementBinder updateStatementBinder;
  private StatementBinder deleteStatementBinder;
  private boolean deletesInBatch = false;

  public BufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection
  ) {
    this.tableId = tableId;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
    this.recordValidator = RecordValidator.create(config);
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException, TableAlterOrCreateException {
    recordValidator.validate(record);
    final List<SinkRecord> flushed = new ArrayList<>();

    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, record.keySchema())) {
      keySchema = record.keySchema();
      schemaChanged = true;
    }
    if (isNull(record.valueSchema())) {
      // For deletes, value and optionally value schema come in as null.
      // We don't want to treat this as a schema change if key schemas is the same
      // otherwise we flush unnecessarily.
      if (config.deleteEnabled) {
        deletesInBatch = true;
      }
    } else if (Objects.equals(valueSchema, record.valueSchema())) {
      if (config.deleteEnabled && deletesInBatch) {
        // flush so an insert after a delete of same record isn't lost
        flushed.addAll(flush());
      }
    } else {
      // value schema is not null and has changed. This is a real schema change.
      valueSchema = record.valueSchema();
      schemaChanged = true;
    }
    if (schemaChanged || updateStatementBinder == null) {
      // Each batch needs to have the same schemas, so get the buffered records out
      flushed.addAll(flush());

      // re-initialize everything that depends on the record schema
      final SchemaPair schemaPair = new SchemaPair(
          record.keySchema(),
          record.valueSchema()
      );
      fieldsMetadata = FieldsMetadata.extract(
          tableId.tableName(),
          config.pkMode,
          config.pkFields,
          config.fieldsWhitelist,
          schemaPair
      );
      dbStructure.createOrAmendIfNecessary(
          config,
          connection,
          tableId,
          fieldsMetadata
      );
      final String insertSql = getInsertSql("insert");
      // final String deleteSql = getDeleteSql();
      // final String updateSql = getInsertSql("update");

      close();
      updatePreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
      updateStatementBinder = dbDialect.statementBinder(
          updatePreparedStatement,
          config.pkMode,
          schemaPair,
          fieldsMetadata,
          dbStructure.tableDefinition(connection, tableId),
          INSERT
      );
      log.debug("end insert statement");
      /*
      deletePreparedStatement = dbDialect.createPreparedStatement(connection, updateSql);
      deleteStatementBinder = dbDialect.statementBinder(
              deletePreparedStatement,
              config.pkMode,
              schemaPair,
              fieldsMetadata,
              dbStructure.tableDefinition(connection, tableId),
              JdbcSinkConfig.InsertMode.UPDATE
      );
      */
      /*
      if (config.deleteEnabled && nonNull(deleteSql)) {
        deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
        deleteStatementBinder = dbDialect.statementBinder(
            deletePreparedStatement,
            config.pkMode,
            schemaPair,
            fieldsMetadata,
            dbStructure.tableDefinition(connection, tableId),
            config.insertMode
        );
      }
      */
    }
    
    // set deletesInBatch if schema value is not null
    if (isNull(record.value()) && config.deleteEnabled) {
      deletesInBatch = true;
    }

    records.add(record);

    if (records.size() >= config.batchSize) {
      flushed.addAll(flush());
    }
    return flushed;
  }

  // add process
  public SinkRecord processStruct(SinkRecord record, Schema inputSchema, Struct inputStruct) {

    SchemaBuilder builder = SchemaBuilder.struct()
            .name(inputSchema.name())
            .doc(inputSchema.doc())
            .version(inputSchema.version());

    if (null != inputSchema.parameters() && !inputSchema.parameters().isEmpty()) {
      builder.parameters(inputSchema.parameters());
    }

    builder.field("effective_at",  SchemaBuilder.int64());
    //GregorianCalendar defaultDeadTime = new GregorianCalendar(
    //        9999, Calendar.JANUARY, 1, 0, 0, 0);

    // defaultDeadTime.setTimeZone(TimeZone.getTimeZone("UTC"));
    // Schema optionalTsWithDefault = SchemaBuilder.int64();
    builder.field("dead_at",SchemaBuilder.int64());

    for (Field field: inputSchema.fields()) {
      builder.field(field.name(), field.schema());
    }

    Schema outputSchema = builder.build();
    Struct outputStruct = new Struct(outputSchema);

    for (Field field: outputSchema.fields()) {
      if (field.name() == "dead_at" || field.name() == "effective_at") {
        continue;
      }
      outputStruct.put(field.name(), inputStruct.get(field.name()));
    }

    outputStruct.put("effective_at", record.timestamp());
    outputStruct.put("dead_at", 253402185599000L);


    SchemaAndValue transformed = new SchemaAndValue(outputSchema, outputStruct);

    return new SinkRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            transformed.schema(),
            transformed.value(),
            record.timestamp()
    );
  }



  // end process


  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      log.debug("Records is empty");
      return new ArrayList<>();
    }
    log.debug("Flushing {} buffered records", records.size());
    for (SinkRecord record : records) {
      log.debug("schema: {} ", record.valueSchema());
      log.debug("value: {} ", record.value());
      SinkRecord newRecord = processStruct(record, record.valueSchema(), (Struct) record.value());

      log.debug("after schema: {} ", newRecord.valueSchema());
      log.debug("after value: {} ", newRecord.value());

      // 此处需要处理delete的情况

      updateStatementBinder.bindRecord(newRecord);

      // deleteStatementBinder.bindRecord(newRecord);

      // record add column value
      // if (isNull(newRecord.value()) && nonNull(deleteStatementBinder)) {
      // } else {
      // }
    }
    long totalDeleteCount = executeDeletes();

    Optional<Long> totalUpdateCount = executeUpdates();

    final long expectedCount = updateRecordCount();
    log.debug("{} records:{} resulting in totalUpdateCount:{} totalDeleteCount:{}",
        config.insertMode, records.size(), totalUpdateCount, totalDeleteCount
    );
    if (totalUpdateCount.filter(total -> total != expectedCount).isPresent()
        && config.insertMode == INSERT) {
      throw new ConnectException(String.format(
          "Update count (%d) did not sum up to total number of records inserted (%d)",
          totalUpdateCount.get(),
          expectedCount
      ));
    }
    if (!totalUpdateCount.isPresent()) {
      log.info(
          "{} records:{} , but no count of the number of rows it affected is available",
          config.insertMode,
          records.size()
      );
    }

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    deletesInBatch = false;
    return flushedRecords;
  }

  /**
   * @return an optional count of all updated rows or an empty optional if no info is available
   */
  private Optional<Long> executeUpdates() throws SQLException {
    Optional<Long> count = Optional.empty();
    for (int updateCount : updatePreparedStatement.executeBatch()) {
      if (updateCount != Statement.SUCCESS_NO_INFO) {
        count = count.isPresent()
            ? count.map(total -> total + updateCount)
            : Optional.of((long) updateCount);
      }
    }
    return count;
  }

  private long executeDeletes() throws SQLException {
    long totalDeleteCount = 0;
    if (nonNull(deletePreparedStatement)) {
      for (int updateCount : deletePreparedStatement.executeBatch()) {
        if (updateCount != Statement.SUCCESS_NO_INFO) {
          totalDeleteCount += updateCount;
        }
      }
    }
    return totalDeleteCount;
  }

  private long updateRecordCount() {
    return records
        .stream()
        // ignore deletes
        .filter(record -> nonNull(record.value()) || !config.deleteEnabled)
        .count();
  }

  public void close() throws SQLException {
    log.debug(
        "Closing BufferedRecords with updatePreparedStatement: {} deletePreparedStatement: {}",
        updatePreparedStatement,
        deletePreparedStatement
    );
    if (nonNull(updatePreparedStatement)) {
      updatePreparedStatement.close();
      updatePreparedStatement = null;
    }
    if (nonNull(deletePreparedStatement)) {
      deletePreparedStatement.close();
      deletePreparedStatement = null;
    }
  }

  private String getInsertSql(String insertMode) throws SQLException {
    // switch (config.insertMode) {
    switch (insertMode) {
      case "insert":
        log.debug("here is in insert env");
        return dbDialect.buildInsertStatement(
            tableId,
            asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames),
            dbStructure.tableDefinition(connection, tableId)
        );
      case "upsert":
        log.debug("here is in upsert env");
        if (fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
                  + " primary key configuration",
              tableId
          ));
        }
        try {
          return dbDialect.buildUpsertQueryStatement(
              tableId,
              asColumns(fieldsMetadata.keyFieldNames),
              asColumns(fieldsMetadata.nonKeyFieldNames),
              dbStructure.tableDefinition(connection, tableId)
          );
        } catch (UnsupportedOperationException e) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
              tableId,
              dbDialect.name()
          ));
        }
      case "update":
        log.debug("here is in update env");
        Set<String> updateList = new HashSet<>();
        updateList.add("dead_at");
        Set<String> keyList = new HashSet<>();
        for (String name : fieldsMetadata.keyFieldNames) {
          keyList.add(name);
        }
        keyList.add("dead_at");
        return dbDialect.buildUpdateStatement(
            tableId,
            asColumns(keyList),
            asColumns(updateList),
            dbStructure.tableDefinition(connection, tableId)
        );
      default:
        throw new ConnectException("Invalid insert mode");
    }
  }

  private String getDeleteSql() {
    String sql = null;
    if (config.deleteEnabled) {
      switch (config.pkMode) {
        case RECORD_KEY:
          if (fieldsMetadata.keyFieldNames.isEmpty()) {
            throw new ConnectException("Require primary keys to support delete");
          }
          try {
            sql = dbDialect.buildDeleteStatement(
                tableId,
                asColumns(fieldsMetadata.keyFieldNames)
            );
          } catch (UnsupportedOperationException e) {
            throw new ConnectException(String.format(
                "Deletes to table '%s' are not supported with the %s dialect.",
                tableId,
                dbDialect.name()
            ));
          }
          break;

        default:
          throw new ConnectException("Deletes are only supported for pk.mode record_key");
      }
    }
    return sql;
  }

  private Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
        .map(name -> new ColumnId(tableId, name))
        .collect(Collectors.toList());
  }
}
