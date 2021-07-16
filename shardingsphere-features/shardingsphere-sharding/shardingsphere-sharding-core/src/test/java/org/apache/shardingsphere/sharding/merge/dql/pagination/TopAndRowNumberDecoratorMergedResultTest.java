/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.sharding.merge.dql.pagination;

import org.apache.shardingsphere.infra.database.DefaultSchema;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.merge.result.MergedResult;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.sharding.merge.dql.ShardingDQLResultMerger;
import org.apache.shardingsphere.infra.binder.segment.select.groupby.GroupByContext;
import org.apache.shardingsphere.infra.binder.segment.select.orderby.OrderByContext;
import org.apache.shardingsphere.infra.binder.segment.select.pagination.PaginationContext;
import org.apache.shardingsphere.infra.binder.segment.select.projection.ProjectionsContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.item.ProjectionsSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.pagination.limit.LimitSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.pagination.limit.NumberLiteralLimitValueSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.pagination.rownum.NumberLiteralRowNumberValueSegment;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.mysql.dml.MySQLSelectStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.sqlserver.dml.SQLServerSelectStatement;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class TopAndRowNumberDecoratorMergedResultTest {
    
    @Test
    public void assertNextForSkipAll() throws SQLException {
        ShardingDQLResultMerger resultMerger = new ShardingDQLResultMerger(DatabaseTypeRegistry.getActualDatabaseType("SQLServer"));
        Map<String, ShardingSphereMetaData> metaDataMap = new HashMap<>();
        ShardingSphereMetaData metaData = mock(ShardingSphereMetaData.class);
        metaDataMap.put(DefaultSchema.LOGIC_NAME, metaData);
        SQLServerSelectStatement selectStatement = new SQLServerSelectStatement();
        selectStatement.setProjections(new ProjectionsSegment(0, 0));
        selectStatement.setLimit(new LimitSegment(0, 0, new NumberLiteralRowNumberValueSegment(0, 0, Integer.MAX_VALUE, true), null));
        SelectStatementContext selectStatementContext = new SelectStatementContext(metaDataMap, Collections.emptyList(), selectStatement, DefaultSchema.LOGIC_NAME);
        MergedResult actual = resultMerger.merge(Arrays.asList(mockQueryResult(), mockQueryResult(), mockQueryResult(), mockQueryResult()), selectStatementContext, null);
        assertFalse(actual.next());
    }
    
    @Test
    public void assertNextWithoutOffsetWithRowCount() throws SQLException {
        ShardingDQLResultMerger resultMerger = new ShardingDQLResultMerger(DatabaseTypeRegistry.getActualDatabaseType("SQLServer"));
        Map<String, ShardingSphereMetaData> metaDataMap = new HashMap<>();
        ShardingSphereMetaData metaData = mock(ShardingSphereMetaData.class);
        metaDataMap.put(DefaultSchema.LOGIC_NAME, metaData);
        SQLServerSelectStatement selectStatement = new SQLServerSelectStatement();
        selectStatement.setProjections(new ProjectionsSegment(0, 0));
        selectStatement.setLimit(new LimitSegment(0, 0, null, new NumberLiteralLimitValueSegment(0, 0, 5)));
        SelectStatementContext selectStatementContext = new SelectStatementContext(metaDataMap, Collections.emptyList(), selectStatement, DefaultSchema.LOGIC_NAME);
        MergedResult actual = resultMerger.merge(Arrays.asList(mockQueryResult(), mockQueryResult(), mockQueryResult(), mockQueryResult()), selectStatementContext, null);
        for (int i = 0; i < 5; i++) {
            assertTrue(actual.next());
        }
        assertFalse(actual.next());
    }
    
    @Test
    public void assertNextWithOffsetWithoutRowCount() throws SQLException {
        ShardingDQLResultMerger resultMerger = new ShardingDQLResultMerger(DatabaseTypeRegistry.getActualDatabaseType("SQLServer"));
        Map<String, ShardingSphereMetaData> metaDataMap = new HashMap<>();
        ShardingSphereMetaData metaData = mock(ShardingSphereMetaData.class);
        metaDataMap.put(DefaultSchema.LOGIC_NAME, metaData);
        SQLServerSelectStatement selectStatement = new SQLServerSelectStatement();
        selectStatement.setProjections(new ProjectionsSegment(0, 0));
        selectStatement.setLimit(new LimitSegment(0, 0, new NumberLiteralRowNumberValueSegment(0, 0, 2, true), null));
        SelectStatementContext selectStatementContext = new SelectStatementContext(metaDataMap, Collections.emptyList(), selectStatement, DefaultSchema.LOGIC_NAME);
        MergedResult actual = resultMerger.merge(Arrays.asList(mockQueryResult(), mockQueryResult(), mockQueryResult(), mockQueryResult()), selectStatementContext, null);
        for (int i = 0; i < 7; i++) {
            assertTrue(actual.next());
        }
        assertFalse(actual.next());
    }
    
    @Test
    public void assertNextWithOffsetBoundOpenedFalse() throws SQLException {
        ShardingDQLResultMerger resultMerger = new ShardingDQLResultMerger(DatabaseTypeRegistry.getActualDatabaseType("SQLServer"));
        Map<String, ShardingSphereMetaData> metaDataMap = new HashMap<>();
        ShardingSphereMetaData metaData = mock(ShardingSphereMetaData.class);
        metaDataMap.put(DefaultSchema.LOGIC_NAME, metaData);
        SQLServerSelectStatement selectStatement = new SQLServerSelectStatement();
        selectStatement.setProjections(new ProjectionsSegment(0, 0));
        selectStatement.setLimit(new LimitSegment(0, 0, new NumberLiteralRowNumberValueSegment(0, 0, 2, false), new NumberLiteralLimitValueSegment(0, 0, 4)));
        SelectStatementContext selectStatementContext = new SelectStatementContext(metaDataMap, Collections.emptyList(), selectStatement, DefaultSchema.LOGIC_NAME);
        MergedResult actual = resultMerger.merge(Arrays.asList(mockQueryResult(), mockQueryResult(), mockQueryResult(), mockQueryResult()), selectStatementContext, null);
        assertTrue(actual.next());
        assertTrue(actual.next());
        assertFalse(actual.next());
    }
    
    @Test
    public void assertNextWithOffsetBoundOpenedTrue() throws SQLException {
        ShardingDQLResultMerger resultMerger = new ShardingDQLResultMerger(DatabaseTypeRegistry.getActualDatabaseType("SQLServer"));
        Map<String, ShardingSphereMetaData> metaDataMap = new HashMap<>();
        ShardingSphereMetaData metaData = mock(ShardingSphereMetaData.class);
        metaDataMap.put(DefaultSchema.LOGIC_NAME, metaData);
        SQLServerSelectStatement selectStatement = new SQLServerSelectStatement();
        selectStatement.setProjections(new ProjectionsSegment(0, 0));
        selectStatement.setLimit(new LimitSegment(0, 0, new NumberLiteralRowNumberValueSegment(0, 0, 2, true), new NumberLiteralLimitValueSegment(0, 0, 4)));
        SelectStatementContext selectStatementContext = new SelectStatementContext(metaDataMap, Collections.emptyList(), selectStatement, DefaultSchema.LOGIC_NAME);
        MergedResult actual = resultMerger.merge(Arrays.asList(mockQueryResult(), mockQueryResult(), mockQueryResult(), mockQueryResult()), selectStatementContext, null);
        assertTrue(actual.next());
        assertTrue(actual.next());
        assertTrue(actual.next());
        assertFalse(actual.next());
    }
    
    private QueryResult mockQueryResult() throws SQLException {
        QueryResult result = mock(QueryResult.class, RETURNS_DEEP_STUBS);
        when(result.next()).thenReturn(true, true, false);
        return result;
    }
}
