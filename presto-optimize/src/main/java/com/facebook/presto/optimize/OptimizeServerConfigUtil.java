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

import io.airlift.log.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2019/1/24.
 */
public class OptimizeServerConfigUtil {

    private static final Logger log = Logger.get(OptimizeServerConfigUtil.class);

    private OptimizeServerConfigUtil() {}

    private static File open() {
        File directory = new File(System.getProperty("java.io.tmpdir") + File.separatorChar + "prestoPlanTree");
        if (!directory.exists()) {
            directory.mkdir();
        }
        return new File(directory, "optimizeConfig");
    }

    public static List<String> readConfig() {
        List<String> rel = new ArrayList<>();
        ObjectInputStream objinput = null;
        try {
            File objFile = open();
            if (objFile.exists()) {
                objinput = new ObjectInputStream(new FileInputStream(objFile));
                rel = (List<String>) objinput.readObject();
            }
        } catch (Exception e) {
            log.error(e);
        } finally {
            try {
                if (objinput != null)
                    objinput.close();
            } catch (IOException e) {
            }
        }
        return rel;
    }

    public static void writeConfig(List<String> optimizeConfig) {
        ObjectOutputStream objout = null;
        try {
            File objFile = open();
            objout = new ObjectOutputStream(new FileOutputStream(objFile));
            objout.writeObject(optimizeConfig);
        } catch (Exception e) {
            log.error(e);
        } finally {
            try {
                if (objout != null)
                    objout.close();
            } catch (IOException e) {
            }
        }
    }
}
