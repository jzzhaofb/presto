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

import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class MaterializedViewInformationExtractor
        extends DefaultTraversalVisitor<Void, Void>
{
    private final MaterializedViewInfo materializedViewInfo = new MaterializedViewInfo();

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, Void context)
    {
        if (node.getLimit().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Limit clause is not supported in query optimizer");
        }
        if (node.getHaving().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Having clause is not supported in query optimizer");
        }
        return super.visitQuerySpecification(node, context);
    }

    protected Void visitSelect(Select node, Void context)
    {
        super.visitSelect(node, context);
        materializedViewInfo.setDistinct(node.isDistinct());
        return null;
    }

    @Override
    protected Void visitRelation(Relation node, Void context)
    {
        if (!(node instanceof Table)) {
            throw new SemanticException(NOT_SUPPORTED, node, "Relation other than Table is not supported in query optimizer");
        }
        if (materializedViewInfo.getBaseTable().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Only support single table rewrite in query optimizer");
        }
        materializedViewInfo.setBaseTable(Optional.of(node));
        return null;
    }

    @Override
    protected Void visitSingleColumn(SingleColumn node, Void context)
    {
        Expression baseColumnName = node.getExpression();
        materializedViewInfo.addBaseToViewColumn(baseColumnName, node.getAlias().orElse(new Identifier(baseColumnName.toString())));
        materializedViewInfo.addViewToBaseColumn(node.getAlias().orElse(new Identifier(baseColumnName.toString())), baseColumnName);
        return null;
    }

    @Override
    protected Void visitAllColumns(AllColumns node, Void context)
    {
        throw new SemanticException(NOT_SUPPORTED, node, "All columns materialized view is not supported in query optimizer");
    }

    @Override
    protected Void visitSimpleGroupBy(SimpleGroupBy node, Void context)
    {
        ImmutableList.Builder<Expression> groupByExpression = ImmutableList.builder();
        for(Expression expression : node.getExpressions()) {
            if (materializedViewInfo.getBaseToViewColumnMap().containsKey(expression)) {
                groupByExpression.add(expression);
            } else if(expression instanceof Identifier && materializedViewInfo.getViewToBaseColumnMap().containsKey((Identifier) expression)){
                groupByExpression.add(materializedViewInfo.getViewToBaseColumnMap().get(expression));
            } else {
                throw new IllegalStateException(format("Materialized view definition does not contain %s as a column", expression.toString()));
            }
        }
        materializedViewInfo.addToGroupBy(new SimpleGroupBy(groupByExpression.build()));
        return null;
    }

    @Override
    protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
    {
        materializedViewInfo.setWhereClause(Optional.of(node));
        return null;
    }

    @Override
    protected Void visitComparisonExpression(ComparisonExpression node, Void context)
    {
        materializedViewInfo.setWhereClause(Optional.of(node));
        return null;
    }

    public MaterializedViewInfo getMaterializedViewInfo()
    {
        return materializedViewInfo;
    }

    public static final class MaterializedViewInfo
    {
        private final Map<Expression, Identifier> baseToViewColumnMap = new HashMap<>();
        private final Map<Identifier, Expression> viewToBaseColumnMap = new HashMap<>();
        private Optional<Relation> baseTable = Optional.empty();
        private Optional<Expression> whereClause = Optional.empty();
        private Set<GroupingElement> groupBy = new HashSet<>();
        private boolean isDistinct;

        private void addBaseToViewColumn(Expression key, Identifier value)
        {
            baseToViewColumnMap.put(key, value);
        }

        private void addViewToBaseColumn(Identifier key, Expression value)
        {
            viewToBaseColumnMap.put(key, value);
        }

        private void addToGroupBy(GroupingElement groupingElement)
        {
            groupBy.add(groupingElement);
        }

        private void setBaseTable(Optional<Relation> baseTable)
        {
            checkState(!this.baseTable.isPresent());
            this.baseTable = baseTable;
        }

        private void setWhereClause(Optional<Expression> whereClause)
        {
            checkState(!this.whereClause.isPresent());
            this.whereClause = whereClause;
        }

        private void setDistinct(boolean state)
        {
            isDistinct = state;
        }

        private Map<Identifier, Expression> getViewToBaseColumnMap()
        {
            return ImmutableMap.copyOf(viewToBaseColumnMap);
        }

        public Optional<Relation> getBaseTable()
        {
            return baseTable;
        }

        public Map<Expression, Identifier> getBaseToViewColumnMap()
        {
            return ImmutableMap.copyOf(baseToViewColumnMap);
        }

        public Set<GroupingElement> getGroupBy()
        {
            return groupBy;
        }

        public Optional<Expression> getWhereClause()
        {
            return whereClause;
        }

        public boolean isDistinct()
        {
            return isDistinct;
        }
    }
}
