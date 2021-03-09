package com.thingmanage.test;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class StreamingJob {
	public static final long serialVersionUID = 6529685098267757690L;
	public static String GenerateSchema(TableSchema schema) {
		StringBuilder fields = new StringBuilder("");
		List<TableColumn> columns = schema.getTableColumns();
		for (TableColumn column : columns){
			fields.append(column.getName()+ " ");
			fields.append(column.getType());
			fields.append(",");
		};
		String out =fields.toString().replace("*ROWTIME*", "");
		return out.substring(0, out.length() -1);
	}
	public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("192.168.1.70",8081,"/home/machdatum/Downloads/FlinkDependencies/flink-python_2.11-1.12.0.jar","/home/machdatum/Downloads/FlinkDependencies/flink-connector-kafka_2.11-1.12.0.jar","/home/machdatum/Downloads/FlinkDependencies/flink-streaming-java_2.11-1.12.0.jar","/home/machdatum/Downloads/FlinkDependencies/flink-json-1.12.0.jar","/home/machdatum/Downloads/FlinkDependencies/flink-table-planner-blink_2.11-1.12.0.jar","/home/machdatum/Downloads/FlinkDependencies/flink-streaming-scala_2.11-1.12.0.jar","/home/machdatum/Downloads/FlinkDependencies/flink-java-1.12.0.jar","/home/machdatum/Downloads/FlinkDependencies/flink-table-api-java-bridge_2.11-1.12.0.jar","/home/machdatum/Downloads/FlinkDependencies/flink-clients_2.11-1.12.0.jar","/home/machdatum/Downloads/FlinkDependencies/kafka-clients-2.7.0.jar","/home/machdatum/Downloads/FlinkDependencies/flink-connector-elasticsearch7_2.11-1.12.0.jar");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
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
					"'topic' = 'dummyraw', " +
					"'properties.bootstrap.servers' = '192.168.1.70:29092'," +
					"'properties.group.id' = 'testGroup'," +
					"'scan.startup.mode' = 'earliest-offset'," +
					"'format' = 'json'" +
					")");

		tEnv.executeSql("SHOW TABLES").print();
		tEnv.getConfig().getConfiguration().setString("python.files","/home/machdatum/Documents/Generated/FlinkUdf/PyUDF/test.py");
		tEnv.getConfig().getConfiguration().setString("python.executable","python");

		tEnv.executeSql("create temporary system function func1 as 'test.func1' language python");

		Table table1= tEnv.sqlQuery("SELECT func1(Dummy) as b FROM rawdata");

//		Table table1 = tEnv.from("rawdata").select(call("func1",$("Dummy")));
		tEnv.executeSql("CREATE TABLE SinkTable(" +
				GenerateSchema(table1.getSchema()) +
				")" +
				"WITH(" +
				"'connector' = 'elasticsearch-7'," +
				"'hosts' = 'http://192.168.1.70:9200'," +
				"'index' = 'pyflindf'" +
				")");
		table1.executeInsert("SinkTable");
	}
}



