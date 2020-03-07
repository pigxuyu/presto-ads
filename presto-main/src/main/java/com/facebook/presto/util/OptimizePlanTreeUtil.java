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
import org.apache.commons.lang3.RandomStringUtils;
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
        if (planTree.getFrom().get() instanceof Table) {
            ObjectReflectSetUtil.setField(planTree, Optional.of(new AliasedRelation(planTree.getFrom().get(), new Identifier(RandomStringUtils.randomAlphanumeric(10).toLowerCase(Locale.ENGLISH)), null)), "from");
        }
        Table table = (Table) ((AliasedRelation) planTree.getFrom().get()).getRelation();
        String tableAliasName = ((AliasedRelation) planTree.getFrom().get()).getAlias().getValue();
        String fullTableName = (table.getName().getParts().size() == 1 ? sessionCatalog + "." + sessionSchema + "." : "") + table.getName().toString() + (StringUtils.isNotEmpty(tableAliasName) ? "." + tableAliasName : "");
        String catalog = table.getName().getParts().size() == 1 ? sessionCatalog : table.getName().getParts().get(0);
        if (StringUtils.isNotEmpty(catalog) && select != null && OptimizeSettingUtil.isNeedOptimizeCatalog(catalog)) {
            Optional<Expression> where = planTree.getWhere();
            Optional<GroupBy> groupBy = planTree.getGroupBy();
            Optional<Expression> having = planTree.getHaving();
            Optional<OrderBy> orderBy = planTree.getOrderBy();
            Optional<String> limit = planTree.getLimit();

            List<SelectItem> reConstructSelect = new ArrayList<>();
            Set<String> fields = new HashSet<>();
            for (int index = 0;index < select.getSelectItems().size();index++) {
                SelectItem field = select.getSelectItems().get(index);
                if (field instanceof SingleColumn) {
                    SingleColumn singleColumn = (SingleColumn) field;
                    singleColumn = rewriteSingleColumn(singleColumn, catalog, fields);
                    reConstructSelect.add(singleColumn);
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
                ObjectReflectSetUtil.setField(planTree, Optional.empty(), "where");
            }
            if (groupBy.isPresent()) {
                List<String> groups = new ArrayList<>();
                List<GroupingElement> groupElements = groupBy.get().getGroupingElements();
                for (GroupingElement item : groupElements) {
                    for (Node node : item.getChildren()) {
                        if (node instanceof FunctionCall) {
                            if (OptimizeSettingUtil.isKylinCatalog(catalog) || (OptimizeSettingUtil.isDruidCatalog(catalog) && !OptimizeSettingUtil.isDruidPushDownDateUDF((FunctionCall) node))) {
                                throw new SemanticException(SemanticErrorCode.NOT_SUPPORTED, node, "catalog %s group by '%s' cannot be supported", catalog, node.toString());
                            }
                        }
                        groups.add(node.toString());
                    }
                }
                sql.append("group by").append(StringUtils.SPACE).append(StringUtils.join(groups, ",")).append(StringUtils.SPACE);
                ObjectReflectSetUtil.setField(planTree, Optional.empty(), "groupBy");
                if (having.isPresent()) {
                    sql.append("having").append(StringUtils.SPACE).append(having.get().toString().replaceAll("\"", "")).append(StringUtils.SPACE);
                    ObjectReflectSetUtil.setField(planTree, Optional.empty(), "having");
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
                ObjectReflectSetUtil.setField(planTree, Optional.empty(), "orderBy");
            }
            if (limit.isPresent()) {
                sql.append("limit").append(StringUtils.SPACE).append(limit.get());
                ObjectReflectSetUtil.setField(planTree, Optional.empty(), "limit");
            }
            ObjectReflectSetUtil.setField(planTree, new Select(select.getLocation().get(), select.isDistinct(), reConstructSelect), "select");
            allSourceSqls.put(fullTableName, new OptimizeTable(sql.toString(), tableAliasName, new ArrayList<>(fields)));
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

    private static SingleColumn rewriteSingleColumn(SingleColumn singleColumn, String catalog, Set<String> fields) {
        Expression expression = singleColumn.getExpression();
        expression = deepTraversalSelect(expression, catalog, fields);
        return new SingleColumn(expression, singleColumn.getAlias());
    }

    private static Expression deepTraversalSelect(Expression expression, String catalog, Set<String> fields) {
        if (expression instanceof Identifier) {
            fields.add(expression.toString());
        } else if (expression instanceof DereferenceExpression) {
            fields.add(((DereferenceExpression) expression).getField().toString());
        } else if (expression instanceof FunctionCall) {
            FunctionCall functionCall = (FunctionCall) expression;
            if (OptimizeSettingUtil.isDruidCatalog(catalog) && OptimizeSettingUtil.isDruidPushDownDateUDF(functionCall)) {
                List<Expression> arguments = new ArrayList<>();
                arguments.add(expression);
                arguments.add(functionCall.getArguments().get(1));
                arguments.add(new StringLiteral("+08:00"));
                fields.add(new FunctionCall(QualifiedName.of("TIME_PARSE"), arguments).toString().replaceAll("\"", ""));
            } else if (OptimizeSettingUtil.isNeedOptimizeAggUDF(functionCall)) {
                Expression firstArgument = functionCall.getArguments().get(0);
                fields.add(expression.toString().replaceAll("\"", ""));
                expression = firstArgument instanceof DereferenceExpression ? ((DereferenceExpression) firstArgument).getField() : firstArgument;
            } else if (OptimizeSettingUtil.isNeedOptimizeCountUDF(functionCall)) {
                String prefix = functionCall.isDistinct() ? OptimizeConstant.COUNT_DISTINCT : OptimizeConstant.COUNT;
                String identifierName;
                if (functionCall.getArguments().isEmpty()) {
                    identifierName = OptimizeConstant.ALL;
                } else {
                    Expression firstArgument = functionCall.getArguments().get(0);
                    if (firstArgument instanceof DereferenceExpression || firstArgument instanceof Identifier) {
                        identifierName = firstArgument instanceof DereferenceExpression ? ((DereferenceExpression) firstArgument).getField().getValue() : ((Identifier) firstArgument).getValue();
                    } else if (firstArgument instanceof Identifier || firstArgument instanceof DereferenceExpression || firstArgument instanceof LongLiteral) {
                        identifierName = OptimizeConstant.ALL;
                    } else {
                        throw new SemanticException(SemanticErrorCode.NOT_SUPPORTED, expression, "catalog %s select '%s' cannot be supported", catalog, expression.toString());
                    }
                }
                fields.add(prefix + identifierName);
                expression = new Identifier(prefix + identifierName);
            } else {
                List<Expression> arguments = functionCall.getArguments();
                List<Expression> newArguments = new ArrayList<>();
                for (Expression argument : arguments) {
                    newArguments.add(deepTraversalSelect(argument, catalog, fields));
                }
                expression = new FunctionCall(functionCall.getName(), functionCall.getWindow(), functionCall.getFilter(), functionCall.getOrderBy(), functionCall.isDistinct(), newArguments);
            }
        } else if (expression instanceof ArithmeticBinaryExpression) {
            ArithmeticBinaryExpression arithmeticBinaryExpression = (ArithmeticBinaryExpression) expression;
            Expression newLeft = deepTraversalSelect(arithmeticBinaryExpression.getLeft(), catalog, fields);
            Expression newRight = deepTraversalSelect(arithmeticBinaryExpression.getRight(), catalog, fields);
            return new ArithmeticBinaryExpression(arithmeticBinaryExpression.getOperator(), newLeft, newRight);
        } else if (expression instanceof ComparisonExpression) {
            ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
            Expression newLeft = deepTraversalSelect(comparisonExpression.getLeft(), catalog, fields);
            Expression newRight = deepTraversalSelect(comparisonExpression.getRight(), catalog, fields);
            return new ComparisonExpression(comparisonExpression.getOperator(), newLeft, newRight);
        } else if (expression instanceof Cast) {
            Cast cast = (Cast) expression;
            Expression newCast = deepTraversalSelect(cast.getExpression(), catalog, fields);
            return new Cast(newCast, cast.getType(), cast.isSafe(), cast.isTypeOnly());
        } else if (expression instanceof IfExpression) {
            IfExpression ifExpression = (IfExpression) expression;
            Expression newCondition = deepTraversalSelect(ifExpression.getCondition(), catalog, fields);
            Expression newTrueValue = deepTraversalSelect(ifExpression.getTrueValue(), catalog, fields);
            Expression newFalseValue = deepTraversalSelect(ifExpression.getFalseValue().get(), catalog, fields);
            return new IfExpression(newCondition, newTrueValue, newFalseValue);
        } else if (expression instanceof SearchedCaseExpression) {
            SearchedCaseExpression searchedCaseExpression = (SearchedCaseExpression) expression;
            List<WhenClause> whenClauseList = searchedCaseExpression.getWhenClauses();
            List<WhenClause> newWhenClauseList = new ArrayList<>();
            for (WhenClause whenClause : whenClauseList) {
                WhenClause newWhenClause = new WhenClause(deepTraversalSelect(whenClause.getOperand(), catalog, fields), deepTraversalSelect(whenClause.getResult(), catalog, fields));
                newWhenClauseList.add(newWhenClause);
            }
            Optional<Expression> newDefaultValue = searchedCaseExpression.getDefaultValue().isPresent() ? Optional.of(deepTraversalSelect(searchedCaseExpression.getDefaultValue().get(), catalog, fields)) : searchedCaseExpression.getDefaultValue();
            return new SearchedCaseExpression(newWhenClauseList, newDefaultValue);
        }
        return expression;
    }

    public static void optimizeAliasePushDown(PlanNode root, AliasedRelation node) {
        try {
            if (root instanceof TableScanNode) {
                TableHandle th = ((TableScanNode) root).getTable();
                String connector = th.getConnectorId().getCatalogName();
                com.facebook.presto.spi.ConnectorTableHandle handle = th.getConnectorHandle();
                if (OptimizeSettingUtil.isNeedOptimizeCatalog(connector)) {
                    ObjectReflectSetUtil.setField(handle, node.getAlias().getValue(), "tableAliasName");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}