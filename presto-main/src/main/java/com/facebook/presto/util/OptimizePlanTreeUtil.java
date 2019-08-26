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

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.optimize.*;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.*;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class OptimizePlanTreeUtil {

    public static void optimizeQueryPlanTree(String sessionCatalog, String sessionSchema, Statement statement, String queryId) {
        if(statement instanceof Query) {
            ObjectMysqlUtil objectMysqlUtil = null;
            try {
                List<String> jdbcConfig = OptimizeServerConfigUtil.readConfig();
                objectMysqlUtil = ObjectMysqlUtil.open(jdbcConfig.get(0), jdbcConfig.get(1), jdbcConfig.get(2));
                OptimizeObj optimizeObj = objectMysqlUtil.readObject(queryId);
                analyzeQueryBody(sessionCatalog, sessionSchema, optimizeObj.getAllSourceSqls(), ((Query) statement).getQueryBody());
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
        } else if (statement instanceof Explain) {
            optimizeQueryPlanTree(sessionCatalog, sessionSchema, ((Explain) statement).getStatement(), queryId);
        }
    }

    private static void analyzeQueryBody(String sessionCatalog, String sessionSchema, Map<String, OptimizeTable> allSourceSqls, QueryBody queryBody) {
        if (queryBody instanceof Union) {
            Union union = (Union) queryBody;
            List<Relation> relations = union.getRelations();
            for (Relation relation : relations) {
                if (relation instanceof QuerySpecification) {
                    genPlanSourceSql(sessionCatalog, sessionSchema, allSourceSqls, (QuerySpecification) relation);
                } else if (relation instanceof TableSubquery) {
                    TableSubquery tableSubquery = (TableSubquery) relation;
                    genPlanSourceSql(sessionCatalog, sessionSchema, allSourceSqls, (QuerySpecification) tableSubquery.getQuery().getQueryBody());
                }
            }
        } else {
            QuerySpecification planTree = (QuerySpecification) queryBody;
            genPlanSourceSql(sessionCatalog, sessionSchema, allSourceSqls, planTree);
        }
    }

    private static void genPlanSourceSql(String sessionCatalog, String sessionSchema, Map<String, OptimizeTable> allSourceSqls, QuerySpecification planTree) {
        if (planTree.getFrom().isPresent()) {
            Relation from = planTree.getFrom().get();
            if (from instanceof Table) {
                optimizeTable(sessionCatalog, sessionSchema, allSourceSqls, planTree);
            } else if (from instanceof TableSubquery) {
                TableSubquery tableSubquery = (TableSubquery) from;
                analyzeQueryBody(sessionCatalog, sessionSchema, allSourceSqls, tableSubquery.getQuery().getQueryBody());
            } else if (from instanceof AliasedRelation) {
                AliasedRelation aliasedRelation = (AliasedRelation) from;
                if (aliasedRelation.getRelation() instanceof TableSubquery) {
                    analyzeQueryBody(sessionCatalog, sessionSchema, allSourceSqls, ((TableSubquery) aliasedRelation.getRelation()).getQuery().getQueryBody());
                } else if (aliasedRelation.getRelation() instanceof Table) {
                    optimizeTable(sessionCatalog, sessionSchema, allSourceSqls, planTree);
                }
            } else if (from instanceof Join) {
                deepTraversalJoin(sessionCatalog, sessionSchema, allSourceSqls, (Join) from);
            }
        }
    }

    private static void optimizeTable(String sessionCatalog, String sessionSchema, Map<String, OptimizeTable> allSourceSqls, QuerySpecification planTree) {
        Select select = planTree.getSelect();
        Table table = planTree.getFrom().get() instanceof Table ? (Table) planTree.getFrom().get() : (Table) ((AliasedRelation) planTree.getFrom().get()).getRelation();
        String tableAliasName = planTree.getFrom().get() instanceof AliasedRelation ? ((AliasedRelation) planTree.getFrom().get()).getAlias().getValue() : "";
        String fullTableName = (table.getName().getParts().size() == 1 ? sessionCatalog + "." + sessionSchema + "." : "") + table.getName().toString() + (StringUtils.isNotEmpty(tableAliasName) ? "." + tableAliasName : "");
        String catalog = table.getName().getParts().size() == 1 ? sessionCatalog : table.getName().getParts().get(0);
        if (StringUtils.isNotEmpty(catalog) && select != null) {
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
                        if (OptimizeSettingUtil.isNeedOptimizeCatalog(catalog)) {
                            if (functionCall.getArguments().size() == 0) {
                                throw new SemanticException(SemanticErrorCode.NOT_SUPPORTED, singleColumn, "catalog kylin or druid '%s' cannot be supported in group by", "count(*)");
                            }
                            if (OptimizeSettingUtil.isDruidCatalog(catalog) &&
                                    functionCall.getName().toString().equalsIgnoreCase("format_datetime") &&
                                    ((functionCall.getArguments().get(0) instanceof Identifier && ((Identifier) functionCall.getArguments().get(0)).getValue().equalsIgnoreCase("__time")) ||
                                    (functionCall.getArguments().get(0) instanceof DereferenceExpression && ((DereferenceExpression) functionCall.getArguments().get(0)).getField().getValue().equalsIgnoreCase("__time")))) {
                                reConstructSelect.add(singleColumn);
                                List<Expression> arguments = new ArrayList<>();
                                arguments.add(singleColumn.getExpression());
                                arguments.add(functionCall.getArguments().get(1));
                                arguments.add(new StringLiteral("+08:00"));
                                fields.add(new FunctionCall(QualifiedName.of("TIME_PARSE"), arguments).toString().replaceAll("\"", ""));
                            } else {
                                Expression column = deepTraversalFunctionCall(functionCall);
                                reConstructSelect.add(new SingleColumn(column, singleColumn.getAlias()));
                                fields.add(singleColumn.getExpression().toString().replaceAll("\"", ""));
                            }
                        } else {
                            reConstructSelect.add(singleColumn);
                            fields.add(singleColumn.getExpression().toString().replaceAll("\"", ""));
                        }
                    } else {
                        reConstructSelect.add(singleColumn);
                        fields.add(singleColumn.getExpression().toString());
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
            sql.append("{columns}").append(StringUtils.SPACE);
            sql.append("from").append(StringUtils.SPACE).append("{tableName}").append(StringUtils.SPACE);
            if (where.isPresent()) {
                sql.append("where").append(StringUtils.SPACE).append(where.get().toString().replaceAll("\"", "")).append(StringUtils.SPACE);
                if (OptimizeSettingUtil.isNeedOptimizeCatalog(catalog)) {
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
                if (OptimizeSettingUtil.isNeedOptimizeCatalog(catalog)) {
                    planTree.setGroupBy(Optional.empty());
                }
                if (having.isPresent()) {
                    sql.append("having").append(StringUtils.SPACE).append(having.get().toString().replaceAll("\"", "")).append(StringUtils.SPACE);
                    if (OptimizeSettingUtil.isNeedOptimizeCatalog(catalog)) {
                        planTree.setHaving(Optional.empty());
                    }
                }
            }
            if (orderBy.isPresent()) {
                List<String> sorts = new ArrayList<>();
                List<SortItem> sortItems = orderBy.get().getSortItems();
                for (SortItem item : sortItems) {
                    if (item.getSortKey() instanceof Identifier) {
                        String sortKey = item.getSortKey().toString();
                        for (int index = 0;index < select.getSelectItems().size();index++) {
                            SelectItem field = select.getSelectItems().get(index);
                            if (field instanceof SingleColumn) {
                                SingleColumn singleColumn = (SingleColumn) field;
                                if (singleColumn.getExpression() instanceof Identifier) {
                                    if (sortKey.equalsIgnoreCase(((Identifier) singleColumn.getExpression()).getValue()) || (singleColumn.getAlias().isPresent() && sortKey.equalsIgnoreCase(singleColumn.getAlias().get().getValue()))) {
                                        sorts.add(singleColumn.getExpression().toString().replaceAll("\"", "") + StringUtils.SPACE + (item.getOrdering() == SortItem.Ordering.DESCENDING ? "desc" : ""));
                                        break;
                                    }
                                } else if (singleColumn.getExpression() instanceof FunctionCall) {
                                    if (singleColumn.getAlias().isPresent() && sortKey.equalsIgnoreCase(singleColumn.getAlias().get().getValue())) {
                                        sorts.add(singleColumn.getExpression().toString().replaceAll("\"", "") + StringUtils.SPACE + (item.getOrdering() == SortItem.Ordering.DESCENDING ? "desc" : ""));
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        sorts.add(item.getSortKey().toString().replaceAll("\"", "") + StringUtils.SPACE + (item.getOrdering() == SortItem.Ordering.DESCENDING ? "desc" : ""));
                    }
                }
                if (sorts.size() > 0) sql.append("order by").append(StringUtils.SPACE).append(StringUtils.join(sorts, ",")).append(StringUtils.SPACE);
                if (OptimizeSettingUtil.isNeedOptimizeCatalog(catalog)) {
                    planTree.setOrderBy(Optional.empty());
                }
            }
            if (limit.isPresent()) {
                sql.append("limit").append(StringUtils.SPACE).append(limit.get());
                if (OptimizeSettingUtil.isNeedOptimizeCatalog(catalog)) {
                    planTree.setLimit(Optional.empty());
                }
            }
            if (OptimizeSettingUtil.isNeedOptimizeCatalog(catalog)) {
                planTree.setSelect(new Select(select.getLocation().get(), select.isDistinct(), reConstructSelect));
            }
            allSourceSqls.put(fullTableName, new OptimizeTable(sql.toString(), tableAliasName, fields));
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

    private static Expression deepTraversalFunctionCall(FunctionCall functionCall) {
        List<Expression> args = functionCall.getArguments();
        for (Expression arg : args) {
            if (arg instanceof Identifier) {
                return arg;
            } else if (arg instanceof FunctionCall) {
                return deepTraversalFunctionCall((FunctionCall) arg);
            } else if (arg instanceof DereferenceExpression) {
                return ((DereferenceExpression) arg).getField();
            }
        }
        return args.get(0);
    }

    public static void optimizeAliasePushDown(PlanNode root, AliasedRelation node) {
        try {
            if (root instanceof TableScanNode) {
                TableHandle th = ((TableScanNode) root).getTable();
                String connector = th.getConnectorId().getCatalogName();
                com.facebook.presto.spi.ConnectorTableHandle handle = th.getConnectorHandle();
                if (OptimizeSettingUtil.isNeedOptimizeCatalog(connector)) {
                    java.lang.reflect.Field tableAliasName = handle.getClass().getDeclaredField("tableAliasName");
                    tableAliasName.setAccessible(true);
                    tableAliasName.set(handle, node.getAlias().getValue());
                    tableAliasName.setAccessible(false);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
