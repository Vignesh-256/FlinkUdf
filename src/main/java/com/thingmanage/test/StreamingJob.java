/*
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

package com.thingmanage.test;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;


public class StreamingJob {

	public static void main(String[] args) throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		TableEnvironment tEnv = TableEnvironment.create(settings);
		tEnv.executeSql("" +
				"CREATE TABLE rawdata" +
				"(" +
				"Cnt INT, " +
				"Dummy STRING, " +
				"Device INT " +
				")" +
				"WITH (" +
				"'connector' = 'kafka', " +
				"'topic' = 'raw', " +
				"'properties.bootstrap.servers' = '192.168.1.130:29092'," +
				"'properties.group.id' = 'testGroup'," +
				"'scan.startup.mode' = 'earliest-offset'," +
				"'format' = 'json'" +
				")");

		tEnv.executeSql("SHOW TABLES").print();
//		tEnv.executeSql("SELECT Cnt,Device FROM rawdata").print();

		tEnv.getConfig().getConfiguration().setString("python.files", "/home/aurora/IdeaProjects/testApplication/test.py");
		tEnv.getConfig().getConfiguration().setString("python.client.executable", "python");
		tEnv.executeSql("create temporary system function func1 as 'test.func1' language python");
        String[] a = tEnv.listUserDefinedFunctions();
		tEnv.executeSql("SELECT func1(Dummy) as b FROM rawdata").print();

//		Table table = tEnv.fromDataSet(env.fromElements("1", "2", "3")).as("str").select("func1(str)");
//		tEnv.toDataSet(table, String.class).collect();



	}
}
