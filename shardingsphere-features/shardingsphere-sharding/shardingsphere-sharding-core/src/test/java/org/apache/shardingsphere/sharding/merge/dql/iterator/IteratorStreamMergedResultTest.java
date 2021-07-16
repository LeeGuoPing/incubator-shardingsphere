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

package org.apache.shardingsphere.sharding.merge.dql.iterator;

import org.apache.shardingsphere.infra.database.DefaultSchema;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.sharding.merge.dql.ShardingDQLResultMerger;
import org.apache.shardingsphere.infra.binder.segment.select.groupby.GroupByContext;
import org.apache.shardingsphere.infra.binder.segment.select.orderby.OrderByContext;
import org.apache.shardingsphere.infra.binder.segment.select.pagination.PaginationContext;
import org.apache.shardingsphere.infra.binder.segment.select.projection.ProjectionsContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.item.ProjectionsSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.pagination.limit.LimitSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.pagination.rownum.NumberLiteralRowNumberValueSegment;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.mysql.dml.MySQLSelectStatement;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.merge.result.MergedResult;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.sqlserver.dml.SQLServerSelectStatement;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class IteratorStreamMergedResultTest {
    
    private SelectStatementContext selectStatementContext;
    
    @Before
    public void setUp() {
        Map<String, ShardingSphereMetaData> metaDataMap = new HashMap<>();
        ShardingSphereMetaData metaData = mock(ShardingSphereMetaData.class);
        metaDataMap.put(DefaultSchema.LOGIC_NAME, metaData);
        MySQLSelectStatement selectStatement = new MySQLSelectStatement();
        selectStatement.setProjections(new ProjectionsSegment(0, 0));
        selectStatement.setLimit(new LimitSegment(0, 0, null, null));
        selectStatementContext = new SelectStatementContext(metaDataMap, Collections.emptyList(), selectStatement, DefaultSchema.LOGIC_NAME);
    }
    
    @Test
    public void assertNextForResultSetsAllEmpty() throws SQLException {
        List<QueryResult> queryResults = Arrays.asList(mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS));
        ShardingDQLResultMerger resultMerger = new ShardingDQLResultMerger(DatabaseTypeRegistry.getActualDatabaseType("MySQL"));
        MergedResult actual = resultMerger.merge(queryResults, selectStatementContext, null);
        assertFalse(actual.next());
    }
    
    @Test
    public void assertNextForResultSetsAllNotEmpty() throws SQLException {
        List<QueryResult> queryResults = Arrays.asList(mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS));
        for (QueryResult each : queryResults) {
            when(each.next()).thenReturn(true, false);
        }
        ShardingDQLResultMerger resultMerger = new ShardingDQLResultMerger(DatabaseTypeRegistry.getActualDatabaseType("MySQL"));
        MergedResult actual = resultMerger.merge(queryResults, selectStatementContext, null);
        assertTrue(actual.next());
        assertTrue(actual.next());
        assertTrue(actual.next());
        assertFalse(actual.next());
    }
    
    @Test
    public void assertNextForFirstResultSetsNotEmptyOnly() throws SQLException {
        List<QueryResult> queryResults = Arrays.asList(mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS));
        when(queryResults.get(0).next()).thenReturn(true, false);
        ShardingDQLResultMerger resultMerger = new ShardingDQLResultMerger(DatabaseTypeRegistry.getActualDatabaseType("MySQL"));
        MergedResult actual = resultMerger.merge(queryResults, selectStatementContext, null);
        assertTrue(actual.next());
        assertFalse(actual.next());
    }
    
    @Test
    public void assertNextForMiddleResultSetsNotEmpty() throws SQLException {
        List<QueryResult> queryResults = Arrays.asList(mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS));
        when(queryResults.get(1).next()).thenReturn(true, false);
        ShardingDQLResultMerger resultMerger = new ShardingDQLResultMerger(DatabaseTypeRegistry.getActualDatabaseType("MySQL"));
        MergedResult actual = resultMerger.merge(queryResults, selectStatementContext, null);
        assertTrue(actual.next());
        assertFalse(actual.next());
    }
    
    @Test
    public void assertNextForLastResultSetsNotEmptyOnly() throws SQLException {
        List<QueryResult> queryResults = Arrays.asList(mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS));
        when(queryResults.get(2).next()).thenReturn(true, false);
        ShardingDQLResultMerger resultMerger = new ShardingDQLResultMerger(DatabaseTypeRegistry.getActualDatabaseType("MySQL"));
        MergedResult actual = resultMerger.merge(queryResults, selectStatementContext, null);
        assertTrue(actual.next());
        assertFalse(actual.next());
    }
    
    @Test
    public void assertNextForMix() throws SQLException {
        List<QueryResult> queryResults = Arrays.asList(mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS), 
                mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS), mock(QueryResult.class, RETURNS_DEEP_STUBS));
        when(queryResults.get(1).next()).thenReturn(true, false);
        when(queryResults.get(3).next()).thenReturn(true, false);
        when(queryResults.get(5).next()).thenReturn(true, false);
        ShardingDQLResultMerger resultMerger = new ShardingDQLResultMerger(DatabaseTypeRegistry.getActualDatabaseType("MySQL"));
        MergedResult actual = resultMerger.merge(queryResults, selectStatementContext, null);
        assertTrue(actual.next());
        assertTrue(actual.next());
        assertTrue(actual.next());
        assertFalse(actual.next());
    }
}
