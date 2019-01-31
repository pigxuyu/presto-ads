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

import java.util.Map;

/**
 * Created by Administrator on 2019/1/25.
 */
public class OptimizeObj {

    private Map<String, OptimizeTable> allSourceSqls;

    private Map<String, String> allWhereCondition;

    public Map<String, OptimizeTable> getAllSourceSqls() {
        return allSourceSqls;
    }

    public void setAllSourceSqls(Map<String, OptimizeTable> allSourceSqls) {
        this.allSourceSqls = allSourceSqls;
    }

    public Map<String, String> getAllWhereCondition() {
        return allWhereCondition;
    }

    public void setAllWhereCondition(Map<String, String> allWhereCondition) {
        this.allWhereCondition = allWhereCondition;
    }
}
