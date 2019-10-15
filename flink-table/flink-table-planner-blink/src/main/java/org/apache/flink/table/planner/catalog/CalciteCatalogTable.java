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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.operations.DataStreamQueryOperation;
import org.apache.flink.table.planner.operations.RichTableSourceQueryOperation;
import org.apache.flink.table.planner.plan.schema.FlinkTable;
import org.apache.flink.table.planner.plan.schema.TableSinkTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.table.util.CatalogTableStatisticsConverter.convertToTableStats;

/**
 * Represents a {@link CatalogBaseTable} in Calcite.
 *
 * <p>This table would be converted to
 * {@link org.apache.flink.table.planner.plan.schema.TableSourceTable}
 * during sql-to-rel conversion. See
 * {@link org.apache.flink.table.planner.plan.schema.FlinkRelOptTable#toRel}
 * and {@link #translateTable} for details.
 */
public class CalciteCatalogTable extends FlinkTable {
	//~ Instance fields --------------------------------------------------------
	private final String catalogName;
	private final ObjectPath tablePath;
	private final Catalog catalog;
	private final CatalogBaseTable catalogTable;
	private final boolean isStreamingMode;

	//~ Constructors -----------------------------------------------------------
	public CalciteCatalogTable(
			String catalogName,
			ObjectPath objectPath,
			Catalog catalog,
			CatalogBaseTable catalogBaseTable,
		boolean isStreaming) {
		this.catalogName = catalogName;
		this.tablePath = objectPath;
		this.catalog = catalog;
		this.catalogTable = catalogBaseTable;
		this.isStreamingMode = isStreaming;
	}

	//~ Methods ----------------------------------------------------------------

	public CatalogBaseTable getCatalogTable() {
		return catalogTable;
	}

	@Override
	public FlinkTable copy(FlinkStatistic statistic) {
		return this;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		assert typeFactory instanceof FlinkTypeFactory;
		final RelDataType retType = ((FlinkTypeFactory) typeFactory).buildRelNodeRowType(
			scala.collection.JavaConversions.asScalaBuffer(
				Arrays.asList(catalogTable.getSchema().getFieldNames())).seq(),
			scala.collection.JavaConversions.asScalaBuffer(
				Arrays.asList(Arrays.stream(catalogTable.getSchema().getFieldDataTypes())
					.map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
					.toArray(LogicalType[]::new))));
		// If the table source is bounded, materialize the time attributes to normal TIMESTAMP type.
		// Now for ConnectorCatalogTable, there is no way to
		// deduce if it is bounded in the table environment, so the data types in TableSchema
		// always patched with TimeAttribute even .
		// See ConnectorCatalogTable#calculateSourceSchema
		// for details.

		// Remove the patched time attributes type to let the TableSourceTable handle it.
		// We should remove this logic if the isBatch flag in ConnectorCatalogTable is fixed.
		if (!isStreamingMode
			&& catalogTable instanceof ConnectorCatalogTable
			&& ((ConnectorCatalogTable) catalogTable).getTableSource().isPresent()) {
			final List<RelDataType> fieldTypes = retType.getFieldList().stream()
				.map(f -> {
					final RelDataType fieldType = f.getType();
					if (fieldType instanceof TimeIndicatorRelDataType) {
						return typeFactory.createTypeWithNullability(
							typeFactory.createSqlType(
								SqlTypeName.TIMESTAMP,
								fieldType.getPrecision()),
							fieldType.isNullable());
					} else {
						return fieldType;
					}
				}).collect(Collectors.toList());
			return typeFactory.createStructType(fieldTypes, retType.getFieldNames());
		} else {
			return retType;
		}
	}

	@Override
	public FlinkStatistic getStatistic() {
		try {
			return FlinkStatistic.builder()
				.tableStats(getTableStatistics(catalog, tablePath))
				.build();
		} catch (TableNotExistException e) {
			throw new TableException(format(
				"A failure occurred when accessing table. Table path [%s, %s, %s]",
				catalogName,
				tablePath.getDatabaseName(),
				tablePath.getObjectName()), e);
		}
	}

	/**
	 * Translate this table to Flink specific table, i.e.
	 * {@link TableSourceTable} or {@link TableSinkTable}.
	 *
	 * @return A translated {@link FlinkTable} instance.
	 */
	public Table translateTable() {
		try {
			String dbName = tablePath.getDatabaseName();
			String tableName = tablePath.getObjectName();
			// TODO supports GenericCatalogView
			if (catalogTable instanceof QueryOperationCatalogView) {
				QueryOperationCatalogView view = (QueryOperationCatalogView) catalogTable;
				QueryOperation operation = view.getQueryOperation();
				if (operation instanceof DataStreamQueryOperation) {
					List<String> qualifiedName = Arrays.asList(catalogName, dbName, tableName);
					((DataStreamQueryOperation) operation).setQualifiedName(qualifiedName);
				} else if (operation instanceof RichTableSourceQueryOperation) {
					List<String> qualifiedName = Arrays.asList(catalogName, dbName, tableName);
					((RichTableSourceQueryOperation) operation).setQualifiedName(qualifiedName);
				}
				return QueryOperationCatalogViewTable.createCalciteTable(view);
			} else if (catalogTable instanceof ConnectorCatalogTable) {
				return convertConnectorTable((ConnectorCatalogTable<?, ?>) catalogTable, tablePath);
			} else if (catalogTable instanceof CatalogTable) {
				return convertCatalogTable(tablePath, (CatalogTable) catalogTable);
			} else {
				throw new TableException("Unsupported table type: " + catalogTable);
			}
		} catch (TableNotExistException | CatalogException e) {
			// TableNotExistException should never happen, because we are checking it exists
			// via catalog.tableExists
			throw new TableException(format(
				"A failure occurred when accessing table. Table path [%s, %s, %s]",
				catalogName,
				tablePath.getDatabaseName(),
				tablePath.getObjectName()), e);
		}
	}

	private Table convertConnectorTable(
		ConnectorCatalogTable<?, ?> table,
		ObjectPath tablePath) throws TableNotExistException {
		if (table.getTableSource().isPresent()) {
			TableSource<?> tableSource = table.getTableSource().get();
			if (!(tableSource instanceof StreamTableSource ||
				tableSource instanceof LookupableTableSource)) {
				throw new ValidationException(
					"Only StreamTableSource and LookupableTableSource can be used in Blink planner.");
			}
			if (!isStreamingMode && tableSource instanceof StreamTableSource &&
				!((StreamTableSource<?>) tableSource).isBounded()) {
				throw new ValidationException("Only bounded StreamTableSource can be used in batch mode.");
			}

			TableStats tableStats = TableStats.UNKNOWN;
			// TODO supports stats for partitionable table
			if (!table.isPartitioned()) {
				tableStats = getTableStatistics(catalog, tablePath);
			}
			return new TableSourceTable<>(
				tableSource,
				isStreamingMode,
				FlinkStatistic.builder().tableStats(tableStats).build());
		} else {
			Optional<TableSinkTable> tableSinkTable = table.getTableSink()
				.map(tableSink -> new TableSinkTable<>(
					tableSink,
					FlinkStatistic.UNKNOWN()));
			if (tableSinkTable.isPresent()) {
				return tableSinkTable.get();
			} else {
				throw new TableException("Cannot convert a connector table " +
					"without either source or sink.");
			}
		}
	}

	private static TableStats getTableStatistics(
			Catalog catalog, ObjectPath tablePath) throws TableNotExistException {
		CatalogTableStatistics tableStatistics = null;
		tableStatistics = catalog.getTableStatistics(tablePath);
		CatalogColumnStatistics columnStatistics = catalog.getTableColumnStatistics(tablePath);
		return convertToTableStats(tableStatistics, columnStatistics);
	}

	private Table convertCatalogTable(ObjectPath tablePath, CatalogTable table) {
		TableSource<?> tableSource;
		Optional<TableFactory> tableFactory = catalog.getTableFactory();
		if (tableFactory.isPresent()) {
			TableFactory tf = tableFactory.get();
			if (tf instanceof TableSourceFactory) {
				tableSource = ((TableSourceFactory) tf).createTableSource(tablePath, table);
			} else {
				throw new TableException(String.format("Cannot query a sink-only table. TableFactory provided by catalog %s must implement TableSourceFactory",
					catalog.getClass()));
			}
		} else {
			tableSource = TableFactoryUtil.findAndCreateTableSource(table);
		}

		if (!(tableSource instanceof StreamTableSource)) {
			throw new TableException("Catalog tables support only StreamTableSource and InputFormatTableSource");
		}

		return new TableSourceTable<>(
			tableSource,
			!((StreamTableSource<?>) tableSource).isBounded(),
			FlinkStatistic.UNKNOWN()
		);
	}
}
