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
package com.facebook.presto.plugin.kylin;

import com.alibaba.fastjson.JSON;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2019/1/24.
 */
public class ObjectMysqlUtil {

    private static String driver = "com.mysql.jdbc.Driver";
    private static String url = "jdbc:mysql://172.21.122.44:3310/test";
    private static String username = "ifly_ad_biz_d";
    private static String password = "A3rLL1GI=H5!BGPftsiqojQx8CpsnR3u";

    private static String queryObjSql = "select objJson from presto_plan_tree where queryId = ?";
    private static String upsertObjSql = "insert into presto_plan_tree (queryId, objJson) values (?, ?) on duplicate key update objJson = ?";

    private Connection conn = null;

    private ObjectMysqlUtil() {}

    public static ObjectMysqlUtil open() throws Exception {
        ObjectMysqlUtil objectMysqlUtil = new ObjectMysqlUtil();
        try {
            Class.forName(driver);
            objectMysqlUtil.conn = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            throw e;
        }
        return objectMysqlUtil;
    }

    @SuppressWarnings("Duplicates")
    public OptimizeObj readObject(String queryId) throws SQLException {
        QueryRunner queryRunner = new QueryRunner();
        OptimizeObj obj = queryRunner.query(conn, queryObjSql, rs -> {
            if (rs.next()) {
                return JSON.parseObject(rs.getString("objJson"), OptimizeObj.class);
            } else {
                OptimizeObj tmp = new OptimizeObj();
                Map<String, OptimizeTable> allSourceSqls = new HashMap<>();
                Map<String, String> allWhereCondition = new HashMap<>();
                tmp.setAllSourceSqls(allSourceSqls);
                tmp.setAllWhereCondition(allWhereCondition);
                return tmp;
            }
        }, new Object[] { queryId });
        return obj;
    }

    public void writeObj(String queryId, OptimizeObj obj) throws SQLException {
        String jsonStr = JSON.toJSONString(obj);
        QueryRunner queryRunner = new QueryRunner();
        queryRunner.update(conn, upsertObjSql, queryId, jsonStr, jsonStr);
    }

    public void close() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
        }
    }
}
