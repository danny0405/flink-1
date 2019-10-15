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

package org.apache.flink.table.api;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * A table column that represents a table column's structure with column name, column data type,
 * computation expression(if it is a computed column) and column strategy.
 */
public class TableColumn {

	//~ Instance fields --------------------------------------------------------

	private final String name;
	private final DataType type;
	private final ColumnExpression expr;
	private final ColumnStrategy strategy;

	//~ Constructors -----------------------------------------------------------

	/**
	 * Creates a {@link TableColumn} instance.
	 *
	 * @param name Column name
	 * @param type Column data type
	 * @param expr Column computation expression if it is a computed column
	 */
	private TableColumn(
		String name,
		@Nullable DataType type,
		@Nullable ColumnExpression expr,
		@Nullable ColumnStrategy strategy) {
		Preconditions.checkArgument(
			type != null || expr != null,
			"Either data type or computation expression must be specified!");
		this.name = Objects.requireNonNull(name, "Column name must be specified!");
		this.type = type;
		this.expr = expr;
		this.strategy = Objects.requireNonNull(strategy,
			"Column strategy must be specified!");
	}

	//~ Methods ----------------------------------------------------------------

	/**
	 * Creates a table column from given name and data type.
	 */
	public static TableColumn of(String name, DataType type) {
		return new TableColumn(
			name,
			type,
			null,
			type.getLogicalType().isNullable()
				? ColumnStrategy.NULLABLE
				: ColumnStrategy.NOT_NULLABLE);
	}

	/**
	 * Creates a table column from given name and computation expression.
	 *
	 * <p>For current implementation, you can only pass in
	 * expression of {@link ColumnExpression} instance.
	 */
	public static TableColumn of(String name, Expression expression) {
		Preconditions.checkArgument(expression instanceof ColumnExpression,
			"");
		return new TableColumn(name, null, (ColumnExpression) expression, ColumnStrategy.VIRTUAL);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TableColumn that = (TableColumn) o;
		return Objects.equals(this.name, that.name)
			&& Objects.equals(this.type, that.type)
			&& Objects.equals(this.expr, that.expr)
			&& Objects.equals(this.strategy, that.strategy);
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.name, this.type, this.expr, this.strategy);
	}

	//~ Getter/Setter ----------------------------------------------------------

	public DataType getType() {
		if (null != type) {
			return type;
		}
		assert null != expr;
		return expr.getOutputDataType();
	}

	public String getName() {
		return name;
	}

	public ColumnExpression getExpr() {
		return expr;
	}

	/**
	 * @return returns the computation expression as serialized string
	 */
	public String getSerializedExpr() {
		return null == expr ? null : expr.asSerializableString();
	}

	public ColumnStrategy getStrategy() {
		return strategy;
	}

	/**
	 * Returns if this column is a computed column(with VIRTUAL strategy).
	 *
	 * @return true if this column is virtual
	 */
	public boolean isVirtual() {
		switch (strategy) {
		case VIRTUAL:
			return true;
		default:
			return false;
		}
	}

	//~ Inner Class ------------------------------------------------------------

	/**
	 * Describe how a column gets populated.
	 */
	public enum ColumnStrategy {
		/**
		 * Column allows null values.
		 * If it was in a projection, it may probably generate nulls.
		 * If it is not specified in SQL INSERT, it will get a NULL value.
		 */
		NULLABLE,
		/**
		 * Column does not allow nulls.
		 * If it was in a projection, it never generates nulls.
		 * You must specify it in the SQL INSERT.
		 */
		NOT_NULLABLE,
		/**
		 * Column is computed and not stored.
		 * You can not insert into it.
		 */
		VIRTUAL
	}

	/**
	 * Column expression that describe the computed column expression.
	 * This class holds a STRING format expression so that it can be serialized and persisted.
	 *
	 * <p>Caution that this class is experimental and the implementation
	 * may change in the future.
	 */
	@Experimental
	public static class ColumnExpression implements ResolvedExpression {

		private final String columnExpr;
		private final DataType type;

		/**
		 * Creates a column expression.
		 *
		 * @param columnExpr Computation expression as STRING format
		 * @param type       Derived type of the expression
		 */
		public ColumnExpression(String columnExpr, DataType type) {
			this.columnExpr = Objects.requireNonNull(columnExpr);
			this.type = Objects.requireNonNull(type);
		}

		@Override
		public DataType getOutputDataType() {
			return this.type;
		}

		@Override
		public String asSerializableString() {
			return this.columnExpr;
		}

		@Override
		public List<ResolvedExpression> getResolvedChildren() {
			return null;
		}

		@Override
		public String asSummaryString() {
			return asSerializableString();
		}

		@Override
		public List<Expression> getChildren() {
			return null;
		}

		@Override
		public <R> R accept(ExpressionVisitor<R> visitor) {
			return null;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof ColumnExpression)) {
				return false;
			}
			ColumnExpression that = (ColumnExpression) obj;
			return Objects.equals(this.columnExpr, that.columnExpr)
				&& Objects.equals(this.type, that.type);
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.columnExpr, this.type);
		}

		@Override
		public String toString() {
			return "expr:[" + this.columnExpr + "] type:[" + this.type + "]";
		}
	}
}
