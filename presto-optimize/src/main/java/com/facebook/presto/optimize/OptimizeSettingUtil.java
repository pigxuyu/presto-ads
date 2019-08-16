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
package com.facebook.presto.optimize;


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
            put("time_format", UdfType.DRUID);
            put("format_datetime", UdfType.DRUID);
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

    private static Map<String, UdfType> getOptimizePushDownUdf() {
        return optimizePushDownUdfMap;
    }

    enum UdfType {
        COMMON, DRUID, KYLIN
    }
}