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


import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Created by Administrator on 2019/8/13.
 */
public class OptimizeSettingUtil {

    private static Map<String, UdfType> optimizePushDownUdfMap = new HashMap<String, UdfType>() {
        {
            put("sum", UdfType.COMMON);
            put("avg", UdfType.COMMON);
            put("max", UdfType.COMMON);
            put("min", UdfType.COMMON);
        }
    };

    public static boolean isNeedOptimizeCatalog(String catalog) {
        return catalog.toLowerCase(Locale.getDefault()).contains("kylin") || catalog.toLowerCase(Locale.getDefault()).contains("druid");
    }

    public static boolean isDruidCatalog(String catalog) {
        return catalog.toLowerCase(Locale.getDefault()).contains("druid");
    }

    public static boolean isKylinCatalog(String catalog) {
        return catalog.toLowerCase(Locale.getDefault()).contains("kylin");
    }

    public static boolean isCountUDF(FunctionCall functionCall) {
        return "count".equalsIgnoreCase(functionCall.getName().toString());
    }

    public static boolean isDruidPushDownDateUDF(FunctionCall functionCall) {
        String functionCallName = functionCall.getName().toString();
        Expression firstArgument = functionCall.getArguments().get(0);
        boolean matchFunctionName = "time_format".equalsIgnoreCase(functionCallName) || "format_datetime".equalsIgnoreCase(functionCallName);
        boolean matchArgument = (firstArgument instanceof Identifier && ((Identifier) firstArgument).getValue().equalsIgnoreCase("__time")) || (firstArgument instanceof DereferenceExpression && ((DereferenceExpression) firstArgument).getField().getValue().equalsIgnoreCase("__time"));
        return matchFunctionName && matchArgument;
    }

    public static boolean isNeedOptimizeUDF(FunctionCall functionCall) {
        String functionCallName = functionCall.getName().toString();
        Expression firstArgument = functionCall.getArguments().get(0);
        boolean matchFunctionName = optimizePushDownUdfMap.containsKey(functionCallName.toLowerCase());
        boolean matchArgument = firstArgument instanceof Identifier || firstArgument instanceof DereferenceExpression;
        return matchFunctionName && matchArgument;
    }

    enum UdfType {
        COMMON, DRUID, KYLIN
    }
}
