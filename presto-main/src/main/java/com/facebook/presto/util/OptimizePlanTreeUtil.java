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
package com.facebook.presto.util;

import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.*;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class OptimizePlanTreeUtil {

    @SuppressWarnings("Duplicates")
    public static void optimizeQueryPlanTree(String sessionCatalog, String sessionSchema, Statement statement, String queryId) {
        if(statement instanceof Query) {
            ObjectMysqlUtil objectMysqlUtil = null;
            try {
                objectMysqlUtil = ObjectMysqlUtil.open();
                OptimizeObj optimizeObj = objectMysqlUtil.readObject(queryId);
                QuerySpecification planTree = (QuerySpecification) ((Query) statement).getQueryBody();
                genPlanSourceSql(sessionCatalog, sessionSchema, optimizeObj.getAllSourceSqls(), planTree);
                objectMysqlUtil.writeObj(queryId, optimizeObj);
            }
            catch (SemanticException e) {
                throw e;
            }
            catch (Exception e) {
                throw new RuntimeException("save external planTree faile", e);
            } finally {
                if (objectMysqlUtil != null)
                    objectMysqlUtil.close();
            }
        }
    }

    private static void genPlanSourceSql(String sessionCatalog, String sessionSchema, Map<String, OptimizeTable> allSourceSqls, QuerySpecification planTree) {
        Relation from = planTree.getFrom().get();
        if (from instanceof Table) {
            optimizeTable(sessionCatalog, sessionSchema, allSourceSqls, planTree);
        } else if (from instanceof TableSubquery) {
            TableSubquery tableSubquery = (TableSubquery) from;
            genPlanSourceSql(sessionCatalog, sessionSchema, allSourceSqls, (QuerySpecification) tableSubquery.getQuery().getQueryBody());
        } else if (from instanceof AliasedRelation) {
            AliasedRelation aliasedRelation = (AliasedRelation) from;
            if (aliasedRelation.getRelation() instanceof TableSubquery) {
                genPlanSourceSql(sessionCatalog, sessionSchema, allSourceSqls, (QuerySpecification) ((TableSubquery) aliasedRelation.getRelation()).getQuery().getQueryBody());
            } else if (aliasedRelation.getRelation() instanceof Table) {
                optimizeTable(sessionCatalog, sessionSchema, allSourceSqls, planTree);
            }
        } else if (from instanceof Join) {
            deepTraversalJoin(sessionCatalog, sessionSchema, allSourceSqls, (Join) from);
        }
    }

    private static void optimizeTable(String sessionCatalog, String sessionSchema, Map<String, OptimizeTable> allSourceSqls, QuerySpecification planTree) {
        Select select = planTree.getSelect();
        Table table = planTree.getFrom().get() instanceof Table ? (Table) planTree.getFrom().get() : (Table) ((AliasedRelation) planTree.getFrom().get()).getRelation();
        String fullTableName = (table.getName().getParts().size() == 1 ? sessionCatalog + "." + sessionSchema + "." : "") + table.getName().toString();
        String catalog = table.getName().getParts().size() == 1 ? sessionCatalog : table.getName().getParts().get(0);
        if (select != null) {
            Optional<Expression> where = planTree.getWhere();
            Optional<GroupBy> groupBy = planTree.getGroupBy();
            Optional<Expression> having = planTree.getHaving();
            Optional<OrderBy> orderBy = planTree.getOrderBy();
            Optional<String> limit = planTree.getLimit();

            List<SelectItem> reConstructSelect = new ArrayList<>();
            List<String> fields = new ArrayList<>();
            for (int index = 0;index < select.getSelectItems().size();index++) {
                SelectItem field = select.getSelectItems().get(index);
                if (field instanceof SingleColumn) {
                    SingleColumn singleColumn = (SingleColumn) field;
                    if (singleColumn.getExpression() instanceof FunctionCall) {
                        FunctionCall functionCall = (FunctionCall) singleColumn.getExpression();
                        if (catalog.toLowerCase(Locale.getDefault()).contains("kylin") || catalog.toLowerCase(Locale.getDefault()).contains("druid")) {
                            Expression column;
                            if (functionCall.getArguments().size() > 0) {
                                column = functionCall.getArguments().get(0);
                            } else {
                                throw new SemanticException(SemanticErrorCode.NOT_SUPPORTED, singleColumn, "kylin or druid '%s' cannot be supported", "count(*)");
                            }
                            reConstructSelect.add(new SingleColumn(column));
                            fields.add(singleColumn.toString().replaceAll("\"", ""));
                        } else {
                            reConstructSelect.add(singleColumn);
                            fields.add(singleColumn.toString().replaceAll("\"", ""));
                        }
                    } else {
                        reConstructSelect.add(singleColumn);
                        fields.add(singleColumn.toString());
                    }
                } else if (field instanceof AllColumns) {
                    reConstructSelect.add(field);
                }
            }

            StringBuilder sql = new StringBuilder();
            sql.append("select").append(StringUtils.SPACE);
            if (select.isDistinct()) {
                sql.append("distinct").append(StringUtils.SPACE);
            }
            if (catalog.toLowerCase(Locale.getDefault()).contains("kylin") || catalog.toLowerCase(Locale.getDefault()).contains("druid")) {
                planTree.setSelect(new Select(select.getLocation().get(), select.isDistinct(), reConstructSelect));
            }
            sql.append("{columns}").append(StringUtils.SPACE);
            sql.append("from").append(StringUtils.SPACE).append("{tableName}").append(StringUtils.SPACE);
            if (where.isPresent()) {
                sql.append("where").append(StringUtils.SPACE).append(where.get().toString().replaceAll("\"", "")).append(StringUtils.SPACE);
                if (catalog.toLowerCase(Locale.getDefault()).contains("kylin") || catalog.toLowerCase(Locale.getDefault()).contains("druid")) {
                    planTree.setWhere(Optional.empty());
                }
            }
            if (groupBy.isPresent()) {
                List<String> groups = new ArrayList<>();
                List<GroupingElement> groupElements = groupBy.get().getGroupingElements();
                for (GroupingElement item : groupElements) {
                    for (Node node : item.getChildren()) {
                        groups.add(node.toString());
                    }
                }
                sql.append("group by").append(StringUtils.SPACE).append(StringUtils.join(groups, ",")).append(StringUtils.SPACE);
                if (catalog.toLowerCase(Locale.getDefault()).contains("kylin") || catalog.toLowerCase(Locale.getDefault()).contains("druid")) {
                    planTree.setGroupBy(Optional.empty());
                }
                if (having.isPresent()) {
                    sql.append("having").append(StringUtils.SPACE).append(having.get().toString().replaceAll("\"", "")).append(StringUtils.SPACE);
                    if (catalog.toLowerCase(Locale.getDefault()).contains("kylin") || catalog.toLowerCase(Locale.getDefault()).contains("druid")) {
                        planTree.setHaving(Optional.empty());
                    }
                }
            }
            if (orderBy.isPresent()) {
                List<String> sorts = new ArrayList<>();
                List<SortItem> sortItems = orderBy.get().getSortItems();
                for (SortItem item : sortItems) {
                    sorts.add(item.getSortKey().toString() + StringUtils.SPACE + (item.getOrdering() == SortItem.Ordering.DESCENDING ? "desc" : ""));
                }
                sql.append("order by").append(StringUtils.SPACE).append(StringUtils.join(sorts, ",")).append(StringUtils.SPACE);
            }
            if (limit.isPresent()) {
                sql.append("limit").append(StringUtils.SPACE).append(limit.get());
            }
            allSourceSqls.put(fullTableName, new OptimizeTable(sql.toString(), fields));
        }
    }

    private static void deepTraversalJoin(String sessionCatalog, String sessionSchema, Map<String, OptimizeTable> allSourceSqls, Join join) {
        if (join.getLeft() instanceof AliasedRelation) {
            AliasedRelation aliasedRelation = (AliasedRelation) join.getLeft();
            if (aliasedRelation.getRelation() instanceof TableSubquery) {
                genPlanSourceSql(sessionCatalog, sessionSchema, allSourceSqls, (QuerySpecification) ((TableSubquery) aliasedRelation.getRelation()).getQuery().getQueryBody());
            }
        } else if (join.getLeft() instanceof Join) {
            deepTraversalJoin(sessionCatalog, sessionSchema, allSourceSqls, (Join) join.getLeft());
        }

        if (join.getRight() instanceof AliasedRelation) {
            AliasedRelation aliasedRelation = (AliasedRelation) join.getRight();
            if (aliasedRelation.getRelation() instanceof TableSubquery) {
                genPlanSourceSql(sessionCatalog, sessionSchema, allSourceSqls, (QuerySpecification) ((TableSubquery) aliasedRelation.getRelation()).getQuery().getQueryBody());
            }
        } else if (join.getRight() instanceof Join) {
            deepTraversalJoin(sessionCatalog, sessionSchema, allSourceSqls, (Join) join.getRight());
        }
    }
}
