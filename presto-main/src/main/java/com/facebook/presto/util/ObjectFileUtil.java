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

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2019/1/24.
 */
public class ObjectFileUtil {

    private File objFile;

    private ObjectFileUtil() {}

    public static ObjectFileUtil open(String queryId) throws IOException {
        ObjectFileUtil objectFileUtil = new ObjectFileUtil();
        try {
            File directory = new File(System.getProperty("java.io.tmpdir") + File.separatorChar + "prestoPlanTree");
            if (!directory.exists()) {
                directory.mkdir();
            }
            objectFileUtil.objFile = new File(directory, queryId);
        } catch (Exception e) {
            throw e;
        }
        return objectFileUtil;
    }

    public OptimizeObj readObject() throws IOException, ClassNotFoundException {
        OptimizeObj obj = new OptimizeObj();
        Map<String, OptimizeTable> allSourceSqls = new HashMap<>();
        Map<String, String> allWhereCondition = new HashMap<>();
        ObjectInputStream objinput = null;
        try {
            if (objFile.exists()) {
                objinput = new ObjectInputStream(new FileInputStream(objFile));
                allSourceSqls = (Map<String, OptimizeTable>) objinput.readObject();
                allWhereCondition = (Map<String, String>) objinput.readObject();
            }
            obj.setAllSourceSqls(allSourceSqls);
            obj.setAllWhereCondition(allWhereCondition);
        } finally {
            if (objinput != null)
                objinput.close();
        }
        return obj;
    }

    public void writeObj(OptimizeObj obj) throws IOException {
        ObjectOutputStream objout = null;
        try {
            objout = new ObjectOutputStream(new FileOutputStream(objFile));
            objout.writeObject(obj.getAllSourceSqls());
            objout.writeObject(obj.getAllWhereCondition());
        } finally {
            if (objout != null)
                objout.close();
        }
    }
}
