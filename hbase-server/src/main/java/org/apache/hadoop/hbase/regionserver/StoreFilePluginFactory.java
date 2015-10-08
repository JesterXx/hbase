/**
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public final class StoreFilePluginFactory {
  private static final Log LOG = LogFactory.getLog(StoreFilePluginFactory.class);

  private StoreFilePluginFactory(){
    //Utility classes should not have a public or default constructor
  }

  static List<StoreFile.Plugin> getPlugins(Configuration conf){
    ArrayList<StoreFile.Plugin> plugins = new ArrayList<StoreFile.Plugin>();
    String classesStr = conf.get(StoreFile.HBASE_HFILE_PLUGINS_KEY);
    if(classesStr != null){
      String[] classNameList = classesStr.split(",");
      for(String className : classNameList){
        className = className.trim();
        try {
          Class<?> cls = Class.forName(className);
          StoreFile.Plugin plugin = (StoreFile.Plugin) cls.newInstance();
          plugin.config(conf);
          plugins.add(plugin);
        } catch (Exception e) {
         LOG.error("Could not instantiate plugin: "+className, e);
        }
      }
    }
    return plugins;
  }
}
