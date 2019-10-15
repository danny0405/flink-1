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

package org.apache.flink.table.planner.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

/**
 * Converts SQL expressions to {@link RexNode}.
 */
public interface SqlToRexConverter {

	/**
	 * Converts a SQL expression to a {@link RexNode} expression.
	 */
	RexNode convertToRexNode(String expr);

	/**
	 * Converts an array of SQL expressions to an array of {@link RexNode} expressions.
	 */
	RexNode[] convertToRexNodes(String[] exprs);

	/**
	 * Parse and validate the sql statement and returns the record type.
	 * You can pass in one atom expression such as "a + b",
	 * or multiple expressions separated with comma, i.e. "a, b, b + 1".
	 *
	 * <p>This method always returns a record type even if there is only one atom expression.
	 *
	 * @param statement the statement to parse and validate
	 * @return record type with each atom expression parsed as field
	 */
	RelDataType getExpressionRowType(String statement);

}
