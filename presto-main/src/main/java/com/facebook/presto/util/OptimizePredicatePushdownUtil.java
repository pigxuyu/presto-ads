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

import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.*;
import org.apache.commons.lang3.StringUtils;

import java.util.Locale;

/**
 * Created by Administrator on 2018/12/11.
 */
public class OptimizePredicatePushdownUtil {

    @SuppressWarnings("Duplicates")
    public static void optimizeDruidRemainingExpression(String tableInfo, Expression remainingExpression, String queryId) {
        String[] schemas = tableInfo.split(":");
        if (schemas[0].toLowerCase(Locale.getDefault()).contains("druid")) {
            String whereCondition = genWhereCondition(remainingExpression);
            if (!StringUtils.isEmpty(whereCondition)) {
                ObjectMysqlUtil objectMysqlUtil = null;
                try {
                    objectMysqlUtil = ObjectMysqlUtil.open();
                    OptimizeObj optimizeObj = objectMysqlUtil.readObject(queryId);
                    optimizeObj.getAllWhereCondition().put(schemas[0] + "." + schemas[1], whereCondition);
                    objectMysqlUtil.writeObj(queryId, optimizeObj);
                }
                catch (SemanticException e) {
                    throw e;
                }
                catch (Exception e) {
                    e.printStackTrace();
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
}
