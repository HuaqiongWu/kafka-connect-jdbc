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

import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.TableDefinition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.PreparedStatement;
import java.sql.SQLException;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;


import static java.util.Objects.isNull;
import java.util.Set;
import java.util.HashSet;


public class PreparedStatementBinder implements StatementBinder {

  private final JdbcSinkConfig.PrimaryKeyMode pkMode;
  private final PreparedStatement statement;
  private final SchemaPair schemaPair;
  private FieldsMetadata fieldsMetadata;
  private final JdbcSinkConfig.InsertMode insertMode;
  private final DatabaseDialect dialect;
  private final TableDefinition tabDef;
  private static final Logger log = LoggerFactory.getLogger(PreparedStatementBinder.class);


  @Deprecated
  public PreparedStatementBinder(
      DatabaseDialect dialect,
      PreparedStatement statement,
      JdbcSinkConfig.PrimaryKeyMode pkMode,
      SchemaPair schemaPair,
      FieldsMetadata fieldsMetadata,
      JdbcSinkConfig.InsertMode insertMode
  ) {
    this(
        dialect,
        statement,
        pkMode,
        schemaPair,
        fieldsMetadata,
        null,
        insertMode
    );
  }

  public PreparedStatementBinder(
      DatabaseDialect dialect,
      PreparedStatement statement,
      JdbcSinkConfig.PrimaryKeyMode pkMode,
      SchemaPair schemaPair,
      FieldsMetadata fieldsMetadata,
      TableDefinition tabDef,
      JdbcSinkConfig.InsertMode insertMode
  ) {
    this.dialect = dialect;
    this.pkMode = pkMode;
    this.statement = statement;
    this.schemaPair = schemaPair;
    this.fieldsMetadata = fieldsMetadata;
    this.insertMode = insertMode;
    this.tabDef = tabDef;
  }

  public SinkRecord processUpdate(SinkRecord record, Schema inputSchema, Struct inputStruct) {

    SchemaBuilder builder = SchemaBuilder.struct()
            .name(inputSchema.name())
            .doc(inputSchema.doc())
            .version(inputSchema.version());

    if (null != inputSchema.parameters() && !inputSchema.parameters().isEmpty()) {
      builder.parameters(inputSchema.parameters());
    }

    for (Field field: inputSchema.fields()) {
      builder.field(field.name(), field.schema());
    }
    Schema outputSchema = builder.build();
    Struct outputStruct = new Struct(outputSchema);

    //for (Field field: outputSchema.fields()) {
    //  outputStruct.put(field.name(), inputStruct.get(field.name()));
    //}
    outputStruct.put("dead_at", inputStruct.get("effective_at"));

    SchemaAndValue transformed = new SchemaAndValue(outputSchema, outputStruct);

    return new SinkRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            transformed.schema(),
            transformed.value(),
            23333
    );
  }

  @Override
  public void bindRecord(SinkRecord record) throws SQLException {
    final Struct valueStruct = (Struct) record.value();
    final boolean isDelete = isNull(valueStruct);
    // Assumption: the relevant SQL has placeholders for keyFieldNames first followed by
    //             nonKeyFieldNames, in iteration order for all INSERT/ UPSERT queries
    //             the relevant SQL has placeholders for keyFieldNames,
    //             in iteration order for all DELETE queries
    //             the relevant SQL has placeholders for nonKeyFieldNames first followed by
    //             keyFieldNames, in iteration order for all UPDATE queries

    int index = 1;
    switch (insertMode) {
      case INSERT:
      case UPSERT:
        log.debug(" in insert case");
        index = bindKeyFields(record, index);
        bindNonKeyFields(record, valueStruct, index);
        break;
      case UPDATE:
        log.debug(" in update case");
        Set<String> updateList = new HashSet<>();
        updateList.add("dead_at");
        Set<String> keyList = new HashSet<>();
        for (String name : fieldsMetadata.keyFieldNames) {
          log.debug("add key {}", name);
          keyList.add(name);
        }
        keyList.add("dead_at");

        SinkRecord updateRecord = processUpdate(record,
                record.valueSchema(), (Struct) record.value());
        index = bindNonKeyFields(updateRecord,
                (Struct) updateRecord.value(), index, updateList);

        bindKeyFields(record, index, keyList);
        break;
      default:
        throw new AssertionError();
    }

    statement.addBatch();
  }

  protected int bindKeyFields(SinkRecord record, int index) throws SQLException {
    switch (pkMode) {
      case NONE:
        if (!fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new AssertionError();
        }
        break;

      case KAFKA: {
        assert fieldsMetadata.keyFieldNames.size() == 3;
        bindField(index++, Schema.STRING_SCHEMA, record.topic(),
            JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(0));
        bindField(index++, Schema.INT32_SCHEMA, record.kafkaPartition(),
            JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(1));
        bindField(index++, Schema.INT64_SCHEMA, record.kafkaOffset(),
            JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(2));
      }
      break;

      case RECORD_KEY: {
        if (schemaPair.keySchema.type().isPrimitive()) {
          assert fieldsMetadata.keyFieldNames.size() == 1;
          bindField(index++, schemaPair.keySchema, record.key(),
              fieldsMetadata.keyFieldNames.iterator().next());
        } else {
          for (String fieldName : fieldsMetadata.keyFieldNames) {
            final Field field = record.keySchema().field(fieldName);
            bindField(index++, field.schema(), ((Struct) record.key()).get(field), fieldName);
          }
        }
      }
      break;

      case RECORD_VALUE: {
        for (String fieldName : fieldsMetadata.keyFieldNames) {
          final Field field = schemaPair.valueSchema.field(fieldName);
          bindField(index++, field.schema(), ((Struct) record.value()).get(field), fieldName);
        }
      }
      break;

      default:
        throw new ConnectException("Unknown primary key mode: " + pkMode);
    }

    return index;
  }

  protected int bindKeyFields(SinkRecord record, int index,
                              Set<String> keyList) throws SQLException {
    switch (pkMode) {
      case RECORD_KEY: {
        if (schemaPair.keySchema.type().isPrimitive()) {
          assert keyList.size() == 1;
          bindField(index++, schemaPair.keySchema, record.key(),
                  keyList.iterator().next());
        } else {
          for (String fieldName : keyList) {
            log.debug("key: {}", fieldName);

            final Field field = record.valueSchema().field(fieldName);
            log.debug("get field: ");
            bindField(index++, field.schema(), ((Struct) record.value()).get(field), fieldName);
          }
        }
      }
      break;

      case RECORD_VALUE: {
        for (String fieldName : fieldsMetadata.keyFieldNames) {
          final Field field = schemaPair.valueSchema.field(fieldName);
          bindField(index++, field.schema(), ((Struct) record.value()).get(field), fieldName);
        }
      }
      break;

      default:
        throw new ConnectException("Unknown primary key mode: " + pkMode);
    }

    return index;
  }

  protected int bindNonKeyFields(
      SinkRecord record,
      Struct valueStruct,
      int index
  ) throws SQLException {
    for (final String fieldName : fieldsMetadata.nonKeyFieldNames) {
      final Field field = record.valueSchema().field(fieldName);
      log.debug("bindNonKey: {}, index: {}", fieldName, index);
      bindField(index++, field.schema(), valueStruct.get(field), fieldName);
    }
    return index;
  }

  protected int bindNonKeyFields(
          SinkRecord record,
          Struct valueStruct,
          int index,
          Set<String> keyList
  ) throws SQLException {
    for (final String fieldName : keyList) {
      final Field field = record.valueSchema().field(fieldName);
      log.debug("bindNonKey: {}, index: {}", fieldName, index);
      bindField(index++, field.schema(), valueStruct.get(field), fieldName);
    }
    return index;
  }

  @Deprecated
  protected void bindField(int index, Schema schema, Object value)
      throws SQLException {
    dialect.bindField(statement, index, schema, value);
  }

  protected void bindField(int index, Schema schema, Object value, String fieldName)
      throws SQLException {
    ColumnDefinition colDef = tabDef == null ? null : tabDef.definitionForColumn(fieldName);
    dialect.bindField(statement, index, schema, value, colDef);
  }
}
