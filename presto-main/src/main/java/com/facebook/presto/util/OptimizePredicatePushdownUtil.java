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

import com.facebook.presto.optimize.ObjectMysqlUtil;
import com.facebook.presto.optimize.OptimizeObj;
import com.facebook.presto.optimize.OptimizeServerConfigUtil;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.*;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Created by Administrator on 2018/12/11.
 */
public class OptimizePredicatePushdownUtil {

    public static void optimizeJoinPredicateExpression(Expression predicate) {
        if (predicate instanceof ComparisonExpression) {
            ComparisonExpression rootExpression = (ComparisonExpression) predicate;
            ComparisonExpression.Operator rootOperator = rootExpression.getOperator();
            Expression rootLeftExpression = rootExpression.getLeft();
            Expression rootRightExpression = rootExpression.getRight();
            if (!rootOperator.equals(ComparisonExpression.Operator.IS_DISTINCT_FROM) && rootLeftExpression instanceof ArithmeticBinaryExpression && isNumberLiteral(rootRightExpression)) {
                ArithmeticBinaryExpression childExpression = (ArithmeticBinaryExpression) rootLeftExpression;
                Expression childLeftExpression = childExpression.getLeft();
                Expression childRightExpression = childExpression.getRight();
                ArithmeticBinaryExpression.Operator childOperator = childExpression.getOperator();

                if (!childOperator.equals(ArithmeticBinaryExpression.Operator.MODULUS) && childLeftExpression instanceof SymbolReference && isNumberLiteral(childRightExpression)) {
                    Double leftValue = rootRightExpression instanceof DoubleLiteral ? getDoubleLiteralVaule(rootRightExpression) : (rootRightExpression instanceof LongLiteral ? getLongLiteralVaule(rootRightExpression) : getGenericLiteralVaule(rootRightExpression));
                    Double rightValue = childRightExpression instanceof DoubleLiteral ? getDoubleLiteralVaule(childRightExpression) : (childRightExpression instanceof LongLiteral ? getLongLiteralVaule(childRightExpression) : getGenericLiteralVaule(childRightExpression));
                    Double finalVaule = simpleArithmeticBinaryExpressionCalculate(leftValue, rightValue, childOperator);
                    rootLeftExpression = childLeftExpression;
                    rootRightExpression = (rootRightExpression instanceof DoubleLiteral || childRightExpression instanceof DoubleLiteral)
                            ? new DoubleLiteral(String.valueOf(finalVaule))
                            : ((rootRightExpression instanceof LongLiteral || childRightExpression instanceof LongLiteral)
                            ? new LongLiteral(String.valueOf(finalVaule.longValue()))
                            : new GenericLiteral(((GenericLiteral) rootRightExpression).getType(), String.valueOf(finalVaule.longValue())));
                    rootExpression.left = rootLeftExpression;
                    rootExpression.right = rootRightExpression;
                }
            } else if (rootOperator.equals(ComparisonExpression.Operator.EQUAL) && rootRightExpression instanceof ArithmeticBinaryExpression && isNumberLiteral(rootLeftExpression)) {
                ArithmeticBinaryExpression childExpression = (ArithmeticBinaryExpression) rootRightExpression;
                Expression childLeftExpression = childExpression.getLeft();
                Expression childRightExpression = childExpression.getRight();
                ArithmeticBinaryExpression.Operator childOperator = childExpression.getOperator();

                if (!childOperator.equals(ArithmeticBinaryExpression.Operator.MODULUS) && childLeftExpression instanceof SymbolReference && isNumberLiteral(childRightExpression)) {
                    Double leftValue = rootLeftExpression instanceof DoubleLiteral ? getDoubleLiteralVaule(rootLeftExpression) : (rootLeftExpression instanceof LongLiteral ? getLongLiteralVaule(rootLeftExpression) : getGenericLiteralVaule(rootLeftExpression));
                    Double rightValue = childRightExpression instanceof DoubleLiteral ? getDoubleLiteralVaule(childRightExpression) : (childRightExpression instanceof LongLiteral ? getLongLiteralVaule(childRightExpression) : getGenericLiteralVaule(childRightExpression));
                    Double finalVaule = simpleArithmeticBinaryExpressionCalculate(leftValue, rightValue, childOperator);
                    rootRightExpression = (rootLeftExpression instanceof DoubleLiteral || rootLeftExpression instanceof DoubleLiteral)
                            ? new DoubleLiteral(String.valueOf(finalVaule))
                            : ((rootLeftExpression instanceof LongLiteral || rootLeftExpression instanceof LongLiteral)
                            ? new LongLiteral(String.valueOf(finalVaule.longValue()))
                            : new GenericLiteral(((GenericLiteral) rootLeftExpression).getType(), String.valueOf(finalVaule.longValue())));
                    rootLeftExpression = childLeftExpression;
                    rootExpression.left = rootLeftExpression;
                    rootExpression.right = rootRightExpression;
                }
            }
        } else if (predicate instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression logicalBinaryExpression = (LogicalBinaryExpression) predicate;
            optimizeJoinPredicateExpression(logicalBinaryExpression.getLeft());
            optimizeJoinPredicateExpression(logicalBinaryExpression.getRight());
        }
    }

    public static void optimizeDruidRemainingExpression(String tableInfo, Expression remainingExpression, String queryId) {
        String[] schemas = tableInfo.split(":");
        if (OptimizeSettingUtil.isDruidCatalog(schemas[0])) {
            String whereCondition = genWhereCondition(remainingExpression);
            if (!StringUtils.isEmpty(whereCondition)) {
                ObjectMysqlUtil objectMysqlUtil = null;
                try {
                    List<String> jdbcConfig = OptimizeServerConfigUtil.readConfig();
                    objectMysqlUtil = ObjectMysqlUtil.open(jdbcConfig.get(0), jdbcConfig.get(1), jdbcConfig.get(2));
                    OptimizeObj optimizeObj = objectMysqlUtil.readObject(queryId);
                    optimizeObj.getAllWhereCondition().put(schemas[0] + "." + schemas[1], whereCondition);
                    objectMysqlUtil.writeObj(queryId, optimizeObj);
                }
                catch (SemanticException e) {
                    throw e;
                }
                catch (Exception e) {
                    throw new RuntimeException("save external predicate push down faile", e);
                } finally {
                    if (objectMysqlUtil != null)
                        objectMysqlUtil.close();
                }
            }
        }
    }

    private static String genWhereCondition(Expression remainingExpression) {
        if (remainingExpression instanceof ComparisonExpression) {
            ComparisonExpression expression = (ComparisonExpression) remainingExpression;
            Expression leftExpression = expression.getLeft();
            Expression rightExpression = expression.getRight();
            String op = expression.getOperator().getValue();
            String left = "1";
            String right = "1";
            if (leftExpression instanceof FunctionCall) {
                FunctionCall functionCall = (FunctionCall) leftExpression;
                if ("format_datetime".equalsIgnoreCase(functionCall.getName().toString().replace("'", "").replace("\"", "")) && "__time".equals(functionCall.getArguments().get(0).toString().replace("'", "").replace("\"", ""))) {
                    left = functionCall.toString().replace("\"", "").replace("format_datetime", "time_format");
                    if (rightExpression instanceof Cast) {
                        Cast cast = (Cast) rightExpression;
                        right = "'" + ((StringLiteral) cast.getExpression()).getValue() + "'";
                    }
                } else if ("time_format".equalsIgnoreCase(functionCall.getName().toString().replace("'", "").replace("\"", "")) && "__time".equals(functionCall.getArguments().get(0).toString().replace("'", "").replace("\"", ""))) {
                    left = functionCall.toString().replace("\"", "");
                    if (rightExpression instanceof Cast) {
                        Cast cast = (Cast) rightExpression;
                        right = "'" + ((StringLiteral) cast.getExpression()).getValue() + "'";
                    }
                } else {
                    op = "=";
                }
            }
            return "(" + left + " " + op + " " + right + ")";
        } else if (remainingExpression instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression expression = (LogicalBinaryExpression) remainingExpression;
            return "(" + genWhereCondition(expression.getLeft()) + " " + expression.getOperator().name() + " " + genWhereCondition(expression.getRight()) + ")";
        }
        return null;
    }

    private static boolean isNumberLiteral(Expression expression) {
        return expression instanceof LongLiteral
                || expression instanceof DoubleLiteral
                || (expression instanceof GenericLiteral && "BIGINT".equalsIgnoreCase(((GenericLiteral) expression).getType()));
    }

    private static Long getLongLiteralVaule(Expression expression) {
        return ((LongLiteral) expression).getValue();
    }

    private static Double getDoubleLiteralVaule(Expression expression) {
        return ((DoubleLiteral) expression).getValue();
    }

    private static Long getGenericLiteralVaule(Expression expression) {
        return Long.valueOf(((GenericLiteral) expression).getValue());
    }

    private static Double simpleArithmeticBinaryExpressionCalculate(Double leftValue, Double rightValue, ArithmeticBinaryExpression.Operator childOperator) {
        Double finalVaule = null;
        switch (childOperator) {
            case ADD:
                finalVaule = leftValue - rightValue;
                break;
            case DIVIDE:
                finalVaule = leftValue * rightValue;
                break;
            case MULTIPLY:
                finalVaule = leftValue / rightValue;
                break;
            case SUBTRACT:
                finalVaule = leftValue + rightValue;
                break;
            default:
                break;
        }
        return finalVaule;
    }
}