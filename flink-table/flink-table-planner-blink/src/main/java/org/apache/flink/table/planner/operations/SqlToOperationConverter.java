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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.SqlToRexConverter;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Mix-in tool class for {@code SqlNode} that allows DDL commands to be
 * converted to {@link Operation}.
 *
 * <p>For every kind of {@link SqlNode}, there needs to have a corresponding
 * #convert(type) method, the 'type' argument should be the subclass
 * of the supported {@link SqlNode}.
 *
 * <p>Every #convert() should return a {@link Operation} which can be used in
 * {@link org.apache.flink.table.delegation.Planner}.
 */
public class SqlToOperationConverter {
	private FlinkPlannerImpl flinkPlanner;

	//~ Constructors -----------------------------------------------------------

	private SqlToOperationConverter(FlinkPlannerImpl flinkPlanner) {
		this.flinkPlanner = flinkPlanner;
	}

	/**
	 * This is the main entrance for executing all kinds of DDL/DML {@code SqlNode}s, different
	 * SqlNode will have it's implementation in the #convert(type) method whose 'type' argument
	 * is subclass of {@code SqlNode}.
	 *
	 * @param flinkPlanner     FlinkPlannerImpl to convertCreateTable sql node to rel node
	 * @param sqlNode          SqlNode to execute on
	 */
	public static Operation convert(FlinkPlannerImpl flinkPlanner, SqlNode sqlNode) {
		// validate the query
		final SqlNode validated = flinkPlanner.validate(sqlNode);
		SqlToOperationConverter converter = new SqlToOperationConverter(flinkPlanner);
		if (validated instanceof SqlCreateTable) {
			return converter.convertCreateTable((SqlCreateTable) validated);
		} if (validated instanceof SqlDropTable) {
			return converter.convertDropTable((SqlDropTable) validated);
		} else if (validated instanceof RichSqlInsert) {
			return converter.convertSqlInsert((RichSqlInsert) validated);
		} else if (validated.getKind().belongsTo(SqlKind.QUERY)) {
			return converter.convertSqlQuery(validated);
		} else {
			throw new TableException("Unsupported node type "
				+ validated.getClass().getSimpleName());
		}
	}

	/**
	 * Convert the {@link SqlCreateTable} node.
	 */
	private Operation convertCreateTable(SqlCreateTable sqlCreateTable) {
		// primary key and unique keys are not supported
		if ((sqlCreateTable.getPrimaryKeyList() != null
				&& sqlCreateTable.getPrimaryKeyList().size() > 0)
			|| (sqlCreateTable.getUniqueKeysList() != null
				&& sqlCreateTable.getUniqueKeysList().size() > 0)) {
			throw new SqlConversionException("Primary key and unique key are not supported yet.");
		}

		// set with properties
		SqlNodeList propertyList = sqlCreateTable.getPropertyList();
		Map<String, String> properties = new HashMap<>();
		if (propertyList != null) {
			propertyList.getList().forEach(p ->
				properties.put(((SqlTableOption) p).getKeyString().toLowerCase(),
					((SqlTableOption) p).getValueString()));
		}

		TableSchema tableSchema = createTableSchema(sqlCreateTable, flinkPlanner.typeFactory());
		String tableComment = "";
		if (sqlCreateTable.getComment() != null) {
			tableComment = sqlCreateTable.getComment().getNlsString().getValue();
		}
		// set partition key
		List<String> partitionKeys = new ArrayList<>();
		SqlNodeList partitionKey = sqlCreateTable.getPartitionKeyList();
		if (partitionKey != null) {
			partitionKeys = partitionKey
				.getList()
				.stream()
				.map(p -> ((SqlIdentifier) p).getSimple())
				.collect(Collectors.toList());
		}
		CatalogTable catalogTable = new CatalogTableImpl(tableSchema,
			partitionKeys,
			properties,
			tableComment);
		return new CreateTableOperation(sqlCreateTable.fullTableName(), catalogTable,
			sqlCreateTable.isIfNotExists());
	}

	/** Convert DROP TABLE statement. */
	private Operation convertDropTable(SqlDropTable sqlDropTable) {
		return new DropTableOperation(sqlDropTable.fullTableName(), sqlDropTable.getIfExists());
	}

	/** Convert insert into statement. */
	private Operation convertSqlInsert(RichSqlInsert insert) {
		// get name of sink table
		List<String> targetTablePath = ((SqlIdentifier) insert.getTargetTable()).names;
		return new CatalogSinkModifyOperation(
			targetTablePath,
			(PlannerQueryOperation) SqlToOperationConverter.convert(flinkPlanner,
				insert.getSource()),
			insert.getStaticPartitionKVs());
	}

	/** Fallback method for sql query. */
	private Operation convertSqlQuery(SqlNode node) {
		return toQueryOperation(flinkPlanner, node);
	}

	//~ Tools ------------------------------------------------------------------

	/**
	 * Create a table schema from {@link SqlCreateTable}. This schema contains computed column
	 * fields, say, we have a create table DDL statement:
	 * <blockquote><pre>
	 *   create table t(
	 *     a int,
	 *     b varchar,
	 *     c as to_timestamp(b))
	 *   with (
	 *     'connector' = 'csv',
	 *     'k1' = 'v1')
	 * </pre></blockquote>
	 *
	 * <p>The returned table schema contains columns (a:int, b:varchar, c:timestamp).
	 *
	 * @param sqlCreateTable sql create table node
	 * @param typeFactory    FlinkTypeFactory instance
	 * @return TableSchema
	 */
	private TableSchema createTableSchema(SqlCreateTable sqlCreateTable,
			FlinkTypeFactory typeFactory) {
		// setup table columns
		SqlNodeList columnList = sqlCreateTable.getColumnList();
		final List<String> physicalNames = new ArrayList<>();
		final List<RelDataType> physicalTypes = new ArrayList<>();
		// collect the physical table schema first.
		final List<SqlNode> physicalColumns = columnList.getList().stream()
			.filter(n -> n instanceof SqlTableColumn).collect(Collectors.toList());
		for (SqlNode node : physicalColumns) {
			SqlTableColumn column = (SqlTableColumn) node;
			final RelDataType relType = column.getType().deriveType(typeFactory,
				column.getType().getNullable());
			physicalNames.add(column.getName().getSimple());
			physicalTypes.add(relType);
		}
		if (!sqlCreateTable.containsComputedColumn()) {
			final TableSchema.Builder builder = new TableSchema.Builder();
			for (int i = 0; i < physicalNames.size(); i++) {
				builder.field(physicalNames.get(i),
					LogicalTypeDataTypeConverter.fromLogicalTypeToDataType(
						FlinkTypeFactory.toLogicalType(physicalTypes.get(i))));
			}
			return builder.build();
		} else {
			final RelDataType rowType = typeFactory.createStructType(physicalTypes, physicalNames);
			final SqlToRexConverter sqlToRexConverter = flinkPlanner.createSqlToRexConverter(rowType);
			final RelDataType logicalRowType = sqlToRexConverter
				.getExpressionRowType(sqlCreateTable.getColumnSqlString());
			// Reset the builder.
			final TableSchema.Builder builder = new TableSchema.Builder();
			final Map<String, String> computedColumnMap =
				sqlCreateTable.getComputedColumnMap();
			logicalRowType.getFieldList().forEach(field -> {
				final String fieldName = field.getName();
				final DataType fieldType = LogicalTypeDataTypeConverter.fromLogicalTypeToDataType(
					FlinkTypeFactory.toLogicalType(field.getType()));
				if (computedColumnMap.containsKey(fieldName)) {
					builder.field(
						fieldName,
						new TableColumn.ColumnExpression(computedColumnMap.get(fieldName), fieldType));
				} else {
					builder.field(fieldName, fieldType);
				}
			});
			return builder.build();
		}
	}

	private PlannerQueryOperation toQueryOperation(FlinkPlannerImpl planner, SqlNode validated) {
		// transform to a relational tree
		RelRoot relational = planner.rel(validated);
		return new PlannerQueryOperation(relational.project());
	}
}
