package com.thingmanage.test;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;



public class StreamingJob {

	public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,settings);

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
		tEnv.getConfig().getConfiguration().setString("python.files","file:///home/machdatum/Documents/flink-1.12.2/test.py");
		tEnv.getConfig().getConfiguration().setString("python.executable","python");

		tEnv.executeSql("create temporary system function func1 as 'test.func1' language python");

		Table table= tEnv.sqlQuery("SELECT func1(Dummy) as b FROM rawdata");
        DataStream<Row> appendStream = tEnv.toAppendStream(table, Row.class);
        System.out.println("Data...."+appendStream.print());
        env.execute("MyFLinkJob");

	}
}



