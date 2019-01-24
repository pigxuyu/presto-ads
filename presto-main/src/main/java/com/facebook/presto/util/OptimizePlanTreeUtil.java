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
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OptimizePlanTreeUtil {

    @SuppressWarnings("Duplicates")
    public static void optimizeQueryPlanTree(String sessionCatalog, String sessionSchema, Statement statement, String queryId) {
        if(statement instanceof com.facebook.presto.sql.tree.Query) {
            java.io.ObjectInputStream objinput = null;
            java.io.ObjectOutputStream objout = null;
            try {
                Map<String, Pair<String, List<String>>> allSourceSqls = new java.util.HashMap<>();
                java.util.Map<String, String> allWhereCondition = new java.util.HashMap<>();
                java.io.File directory = new java.io.File(System.getProperty("java.io.tmpdir") + java.io.File.separatorChar + "prestoPlanTree");
                if (!directory.exists()) {
                    directory.mkdir();
                }
                File objFile = new java.io.File(directory, queryId);
                if (objFile.exists()) {
                    objinput = new java.io.ObjectInputStream(new java.io.FileInputStream(objFile));
                    allSourceSqls = (Map<String, Pair<String, List<String>>>) objinput.readObject();
                    allWhereCondition = (Map<String, String>) objinput.readObject();
                    objinput.close();
                }

                objout = new java.io.ObjectOutputStream(new java.io.FileOutputStream(new java.io.File(directory, queryId)));
                com.facebook.presto.sql.tree.QuerySpecification planTree = (com.facebook.presto.sql.tree.QuerySpecification) ((com.facebook.presto.sql.tree.Query) statement).getQueryBody();
                genPlanSourceSql(sessionCatalog, sessionSchema, allSourceSqls, planTree);
                objout.writeObject(allSourceSqls);
                objout.writeObject(allWhereCondition);
                objout.close();
            }
            catch (SemanticException e) {
                throw e;
            }
            catch (Exception e) {
                throw new RuntimeException("save external planTree faile", e);
            } finally {
                try {
                    if (objinput != null) {
                        objinput.close();
                    }
                    if (objout != null) {
                        objout.close();
                    }
                } catch (Exception ex) {}
            }
        }
    }

    private static void genPlanSourceSql(String sessionCatalog, String sessionSchema, Map<String, Pair<String, List<String>>> allSourceSqls, QuerySpecification planTree) {
        Relation from = planTree.getFrom().get();
        if (from instanceof Table) {
            Select select = planTree.getSelect();
            Table table = (Table) from;
            String fullTableName = (table.getName().getParts().size() == 1 ? sessionCatalog + "." + sessionSchema + "." : "") + table.getName().toString();
            String catalog = table.getName().getParts().size() == 1 ? sessionCatalog : table.getName().getParts().get(0);
            if (select != null) {
                Optional<Expression> where = planTree.getWhere();
                Optional<GroupBy> groupBy = planTree.getGroupBy();
                Optional<Expression> having = planTree.getHaving();
                Optional<OrderBy> orderBy = planTree.getOrderBy();
                Optional<String> limit = planTree.getLimit();

                List<SelectItem> reConstructSelect = new java.util.ArrayList<>();
                List<String> fields = new java.util.ArrayList<>();
                for (int index = 0;index < select.getSelectItems().size();index++) {
                    SelectItem field = select.getSelectItems().get(index);
                    if (field instanceof SingleColumn) {
                        SingleColumn singleColumn = (SingleColumn) field;
                        if (singleColumn.getExpression() instanceof FunctionCall) {
                            FunctionCall functionCall = (FunctionCall) singleColumn.getExpression();
                            if (catalog.toLowerCase(java.util.Locale.getDefault()).contains("kylin") || catalog.toLowerCase(java.util.Locale.getDefault()).contains("druid")) {
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
                sql.append("select").append(org.apache.commons.lang3.StringUtils.SPACE);
                if (select.isDistinct()) {
                    sql.append("distinct").append(org.apache.commons.lang3.StringUtils.SPACE);
                }
                if (catalog.toLowerCase(java.util.Locale.getDefault()).contains("kylin") || catalog.toLowerCase(java.util.Locale.getDefault()).contains("druid")) {
                    planTree.setSelect(new Select(select.getLocation().get(), select.isDistinct(), reConstructSelect));
                }
                sql.append("{columns}").append(org.apache.commons.lang3.StringUtils.SPACE);
                sql.append("from").append(org.apache.commons.lang3.StringUtils.SPACE).append("{tableName}").append(org.apache.commons.lang3.StringUtils.SPACE);
                if (where.isPresent()) {
                    sql.append("where").append(org.apache.commons.lang3.StringUtils.SPACE).append(where.get().toString().replaceAll("\"", "")).append(org.apache.commons.lang3.StringUtils.SPACE);
                    if (catalog.toLowerCase(java.util.Locale.getDefault()).contains("kylin") || catalog.toLowerCase(java.util.Locale.getDefault()).contains("druid")) {
                        planTree.setWhere(Optional.empty());
                    }
                }
                if (groupBy.isPresent()) {
                    List<String> groups = new java.util.ArrayList<>();
                    List<GroupingElement> groupElements = groupBy.get().getGroupingElements();
                    for (GroupingElement item : groupElements) {
                        for (Node node : item.getChildren()) {
                            groups.add(node.toString());
                        }
                    }
                    sql.append("group by").append(org.apache.commons.lang3.StringUtils.SPACE).append(org.apache.commons.lang3.StringUtils.join(groups, ",")).append(org.apache.commons.lang3.StringUtils.SPACE);
                    if (catalog.toLowerCase(java.util.Locale.getDefault()).contains("kylin") || catalog.toLowerCase(java.util.Locale.getDefault()).contains("druid")) {
                        planTree.setGroupBy(Optional.empty());
                    }
                    if (having.isPresent()) {
                        sql.append("having").append(org.apache.commons.lang3.StringUtils.SPACE).append(having.get().toString().replaceAll("\"", "")).append(org.apache.commons.lang3.StringUtils.SPACE);
                        if (catalog.toLowerCase(java.util.Locale.getDefault()).contains("kylin") || catalog.toLowerCase(java.util.Locale.getDefault()).contains("druid")) {
                            planTree.setHaving(Optional.empty());
                        }
                    }
                }
                if (orderBy.isPresent()) {
                    List<String> sorts = new java.util.ArrayList<>();
                    List<SortItem> sortItems = orderBy.get().getSortItems();
                    for (SortItem item : sortItems) {
                        sorts.add(item.getSortKey().toString() + org.apache.commons.lang3.StringUtils.SPACE + (item.getOrdering() == SortItem.Ordering.DESCENDING ? "desc" : ""));
                    }
                    sql.append("order by").append(org.apache.commons.lang3.StringUtils.SPACE).append(org.apache.commons.lang3.StringUtils.join(sorts, ",")).append(org.apache.commons.lang3.StringUtils.SPACE);
                }
                if (limit.isPresent()) {
                    sql.append("limit").append(org.apache.commons.lang3.StringUtils.SPACE).append(limit.get());
                }
                allSourceSqls.put(fullTableName, Pair.of(sql.toString(), fields));
            }
        } else if (from instanceof TableSubquery) {
            TableSubquery tableSubquery = (TableSubquery) from;
            genPlanSourceSql(sessionCatalog, sessionSchema, allSourceSqls, (QuerySpecification) tableSubquery.getQuery().getQueryBody());
        } else if (from instanceof AliasedRelation) {
            AliasedRelation aliasedRelation = (AliasedRelation) from;
            if (aliasedRelation.getRelation() instanceof TableSubquery) {
                genPlanSourceSql(sessionCatalog, sessionSchema, allSourceSqls, (QuerySpecification) ((TableSubquery) aliasedRelation.getRelation()).getQuery().getQueryBody());
            }
        } else if (from instanceof Join) {
            deepTraversalJoin(sessionCatalog, sessionSchema, allSourceSqls, (Join) from);
        }
    }

    private static void deepTraversalJoin(String sessionCatalog, String sessionSchema, Map<String, Pair<String, List<String>>> allSourceSqls, Join join) {
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