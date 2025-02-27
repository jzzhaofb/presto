/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Table;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestMaterializedViewQueryOptimizer
        extends AbstractAnalyzerTest
{
    private static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final String BASE_TABLE_1 = "t1";
    private static final String BASE_TABLE_2 = "t2";
    private static final String BASE_TABLE_6 = "t6";
    private static final String BASE_TABLE_7 = "t7";
    private static final String VIEW = "view";
    private RowExpressionDomainTranslator domainTranslator;

    @BeforeClass
    public void setupDomainTranslator()
    {
        domainTranslator = new RowExpressionDomainTranslator(metadata);
    }

    @Test
    public void testWithSimpleQuery()
    {
        String originalViewSql = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b FROM %s", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);
    }

    @Test
    public void testWithDistinct()
    {
        String originalViewSql = format("SELECT DISTINCT a, b FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT DISTINCT a, b FROM %s", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT DISTINCT a, b FROM %s", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT DISTINCT a, b FROM %s", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT DISTINCT a, b FROM %s", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT DISTINCT a, b FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    @Test
    public void testWithAlias()
    {
        String originalViewSql = format("SELECT a as mv_a, b, c as mv_c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT mv_a, b, mv_c FROM %s", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a as mv_a, b, c as mv_c, d FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a as result_a, b as result_b, c, d FROM %s", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT mv_a as result_a, b as result_b, mv_c, d FROM %s", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);
    }

    @Test
    public void testWithAllColumnsSelect()
    {
        String originalViewSql = format("SELECT * FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT * FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    @Test
    public void testWithBaseQueryGroupBy()
    {
        String originalViewSql = format("SELECT a as mv_a, b, c as mv_c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT SUM(a * b), MAX(a + b), c FROM %s GROUP BY c", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT SUM(mv_a * b), MAX(mv_a + b), mv_c FROM %s GROUP BY mv_c", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);
    }

    @Test
    public void testWithDerivedFields()
    {
        String originalViewSql = format("SELECT SUM(a * b + c) as mv_sum, MAX(a * b + c) as mv_max, d, e FROM %s GROUP BY d, e", BASE_TABLE_1);
        String baseQuerySql = format("SELECT SUM(a * b + c), MAX(a * b + c), d, e FROM %s GROUP BY d, e", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT SUM(mv_sum), MAX(mv_max), d, e FROM %s GROUP BY d, e", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT SUM(a * b + c) as mv_sum, MAX(a * b + c) as mv_max, d as mv_d, e FROM %s GROUP BY d, e", BASE_TABLE_1);
        baseQuerySql = format("SELECT SUM(a * b + c) as sum_of_abc, MAX(a * b + c) as max_of_abc, d, e FROM %s GROUP BY d, e", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT SUM(mv_sum) as sum_of_abc, MAX(mv_max) as max_of_abc, mv_d, e FROM %s GROUP BY mv_d, e", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);
    }

    @Test
    public void testWithArithmeticBinary()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a + b, a * b - c FROM %s", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a + b, a * b - c FROM %s", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a as mv_a, b, c as mv_c, d FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a + b, c / d, a * c - b * d FROM %s", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT mv_a + b, mv_c / d, mv_a * mv_c - b * d FROM %s", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);
    }

    @Test
    public void testWithWhereCondition()
    {
        String originalViewSql = format("SELECT a, b, c, d FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b FROM %s WHERE a < 10 AND c > 10 or d = '2000-01-01'", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b FROM %s WHERE a < 10 AND c > 10 or d = '2000-01-01'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a as mv_a, b, c, d as mv_d FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b FROM %s WHERE a < 10 AND c > 10 or d = '2000-01-01'", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT mv_a, b FROM %s WHERE mv_a < 10 AND c > 10 or mv_d = '2000-01-01'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);
    }

    @Test
    public void testWithOrderBy()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s ORDER BY c ASC, b DESC, a", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b, c FROM %s ORDER BY c ASC, b DESC, a", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a as mv_a, b, c as mv_c FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s ORDER BY c ASC, b DESC, a", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT mv_a, b, mv_c FROM %s ORDER BY mv_c ASC, b DESC, mv_a", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a as mv_a, b, c as mv_c FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s ORDER BY c ASC, b DESC, a", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT mv_a, b, mv_c FROM %s ORDER BY mv_c ASC, b DESC, mv_a", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT MAX(a) as mv_max_a, b FROM %s GROUP BY b", BASE_TABLE_1);
        baseQuerySql = format("SELECT MAX(a), b FROM %s GROUP BY b ORDER BY MAX(a) DESC, b ASC", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT MAX(mv_max_a), b FROM %s GROUP BY b ORDER BY MAX(mv_max_a) DESC, b ASC", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);
    }

    @Test
    public void testWithNoMatchingBaseTable()
    {
        String originalViewSql = format("SELECT a, b FROM %s", BASE_TABLE_2);
        String baseQuerySql = format("SELECT a, b FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    @Test
    public void testWithNoMatchingColumnNames()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT c, d FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, c FROM %s WHERE d = 5", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    @Test
    public void testWithDifferentFilterCondition()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5 OR b = 3", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, c FROM %s WHERE a = 5 OR b = 4", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, c FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    @Test
    public void testWithNoGroupByInBaseQuery()
    {
        String originalViewSql = format("SELECT SUM(a) as sum_a, b FROM %s GROUP BY b", BASE_TABLE_1);
        String baseQuerySql = format("SELECT b FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    @Test
    public void testWithMissingColumnInOrderBy()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, c FROM %s ORDER BY b DESC, d", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    @Test
    public void testWithLimitClause()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s LIMIT 5", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, c FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    // TODO: Handle table alias rewrite for view definition and base query https://github.com/prestodb/presto/issues/16404#issue-940248564
    @Test
    public void testWithTableAlias()
    {
        String originalViewSql = format("SELECT base1.a, b, c FROM %s base1", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, c FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT base1.a, c FROM %s base1", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    @Test
    public void testWithJoinTables()
    {
        String originalViewSql = format(
                "SELECT %s.a, %s.b FROM %s JOIN %s ON %s.c = %s.c",
                BASE_TABLE_1,
                BASE_TABLE_2,
                BASE_TABLE_1,
                BASE_TABLE_2,
                BASE_TABLE_1,
                BASE_TABLE_2);
        String baseQuerySql = format("SELECT a, c FROM %s base1", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        baseQuerySql = format(
                "SELECT %s.a, %s.b FROM %s JOIN %s ON %s.c = %s.c",
                BASE_TABLE_1,
                BASE_TABLE_2,
                BASE_TABLE_1,
                BASE_TABLE_2,
                BASE_TABLE_1,
                BASE_TABLE_2);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    @Test
    public void testFilterContainment()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a >= 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a >= 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a > 5", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 3", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a <> 4", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 3", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a > 5", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 4", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a <> 5", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a >= 5", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a < 3", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 4", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 3", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE c > 5", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b = 5.0", BASE_TABLE_7);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 5.0", BASE_TABLE_7);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 5.0", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b > 5.0", BASE_TABLE_7);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 5.01", BASE_TABLE_7);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 5.01", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b = 'apples'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'apples'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 'apples'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'apples'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 'apples'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b <> 'banana'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b > 'banana'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b > 'banana'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b > 'apples'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b > 'banana'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b > 'banana'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b > '122'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b > '123'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b > '123'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'apples'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b > 'banana'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b > 'banana'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b = 'apples'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    @Test
    public void testFilterContainmentWithAnd()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 0", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 AND a > 0", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5 AND a > 0", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 AND b = 7", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5 AND b = 7", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5 AND c = 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 AND b = 7 AND c = 9", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5 AND b = 7 AND c = 9", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 3 AND a < 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 5 AND a < 7", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a > 5 AND a < 7", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a < 5 AND b > 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 3 AND b > 11", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 3 AND b > 11", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a < 5 AND b > 7 AND c <> 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 3 AND b > 9 AND c = 11", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 3 AND b > 9 AND c = 11", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a <> 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 5 AND a > 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 5 AND a > 5", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE a < 9 AND b > 3.0", BASE_TABLE_7);
        baseQuerySql = format("SELECT a, b FROM %s WHERE a < 7 AND b = 3.1", BASE_TABLE_7);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE a < 7 AND b = 3.1", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <> 'apples' AND b <> 'banana'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b <> 'apples' AND b <> 'banana'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE a > 6 AND b <> 'banana'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE a = 8 AND b = 'apples'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE a = 8 AND b = 'apples'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b = 'orange'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <> 'apples' AND b <> 'banana'", BASE_TABLE_6);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    @Test
    public void testFilterContainmentWithOr()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 7", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a <> 7", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 6", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 6", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a >= 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 6", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 6", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a <> 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 5 OR a > 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 5 OR a > 5", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 3 OR a < 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 5 OR a < 7", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a > 5 OR a < 7", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a < 3 OR a > 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 1 OR a > 11", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 1 OR a > 11", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 3 OR a > 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 9 OR a = 3", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a > 9 OR a = 3", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a < 3 OR b > 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 1 OR b > 11", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 1 OR b > 11", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 3 AND a < 9 OR a > 10", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 5 AND a < 7 OR a > 11", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a > 5 AND a < 7 OR a > 11", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 2.91", BASE_TABLE_7);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <= 2.9 AND b >= 3.0", BASE_TABLE_7);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b <= 2.9 AND b >= 3.0", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'orange'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'apples' OR b = 'banana'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s  WHERE b = 'apples' OR b = 'banana'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 6", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 OR b = 6", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 6", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'apples'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <> 'apples' OR b <> 'banana'", BASE_TABLE_6);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'orange'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <> 'apples' OR b <> 'banana'", BASE_TABLE_6);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    @Test
    public void testFilterContainmentWithIn()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (4,5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (3,4,5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (3,5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (3,5)", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a >= 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a <> 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (4,6)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (4,6)", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (4,5) AND a IN (5,6,7)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (4,5) OR a IN (6,7)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (4,5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (3,5) AND a IN (5,6)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (3,5) AND a IN (5,6)", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5)", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a NOT IN (4,5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (4,5)", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 5 OR a < 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5)", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5,6) AND b IN (6,8)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 5 AND b = 8", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 5 AND b = 8", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b IN ('USA','CAN')", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'CAN' OR b = 'USA'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 'CAN' OR b = 'USA'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b NOT IN ('USA','CAN')", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'ABC'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 'ABC'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5,6,7)", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 7", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5,6)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a <= 5", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5,6)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a NOT IN (6,7)", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5,6)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (6,7)", BASE_TABLE_1);

        assertOptimizedQuery(originalViewSql, baseQuerySql, baseQuerySql);
    }

    // Some of DNF conversions on (A^~B) might not be successful due to exponential explosion of sub-expressions
    // TODO: Implement method that utilizes external SAT solver libraries. https://github.com/prestodb/presto/issues/16536
    @Test(enabled = false)
    public void testFilterContainmentDisjunctiveNormalForm()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 1 AND b = 2 OR b = 3 AND c = 4", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 1 AND b = 2 AND c = 3", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 1 AND b = 2 AND c = 3", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format(
                "SELECT a, b, c FROM %s WHERE " +
                "a = 1 AND b = 2 " +
                "OR b = 3 AND c = 4 " +
                "OR a = 5 AND c = 6",
                BASE_TABLE_1);
        baseQuerySql = format(
                "SELECT a, b, c FROM %s WHERE " +
                "a = 1 AND b = 2 AND c = 3 " +
                "OR a = 5 AND b = 7 AND c = 6",
                BASE_TABLE_1);
        expectedRewrittenSql = format(
                "SELECT a, b, c FROM %s WHERE " +
                "a = 1 AND b = 2 AND c = 3 " +
                "OR a = 5 AND b = 7 AND c = 6",
                VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);
    }

    // Mismatch Domain Type Problem: https://github.com/prestodb/presto/issues/16530
    @Test(enabled = false)
    public void testFilterContainmentWithMismatchStringLength()
    {
        String originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);
        String baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'apple'", BASE_TABLE_6);
        String expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 'apple'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);

        originalViewSql = format("SELECT a, b FROM %s WHERE b NOT IN ('USA','CAN')", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'UK'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 'UK'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedRewrittenSql);
    }

    private void assertOptimizedQuery(String originalViewSql, String baseQuerySql, String expectedViewSql)
    {
        Table viewTable = new Table(QualifiedName.of(VIEW));

        Query originalViewQuery = (Query) SQL_PARSER.createStatement(originalViewSql, PARSING_OPTIONS);
        Query baseQuery = (Query) SQL_PARSER.createStatement(baseQuerySql, PARSING_OPTIONS);
        Query expectedViewQuery = (Query) SQL_PARSER.createStatement(expectedViewSql, PARSING_OPTIONS);

        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(CLIENT_SESSION, session -> {
                    Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer(metadata, session, SQL_PARSER, accessControl, domainTranslator, viewTable, originalViewQuery).rewrite(baseQuery);
                    assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
                });
    }
}
