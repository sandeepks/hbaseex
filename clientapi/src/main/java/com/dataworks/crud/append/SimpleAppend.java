package com.dataworks.crud.append;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class SimpleAppend {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 5, "colfam1", "colfam2");

		util.put("forfun", 
				new String[] { "row1" }, 
				new String[] { "colfam1" }, 
				new String[] { "qual1" },
				new long[] { 1 }, 
				new String[] { "oldvalue" });
		System.out.println("Before get call...");

		util.dump("forfun", new String[] { "row1" }, null, null);
		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		Append append = new Append(Bytes.toBytes("row1"));
		append.setReturnResults(true);
		append.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("newval"));
		append.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("yetanothervalue"));
		
		Result result = tbl.append(append);
		System.out.println("Append Result = "+result);
		
		tbl.close();
		util.dump("forfun", new String[] { "row1" }, null, null);
		util.close();

	}

}
