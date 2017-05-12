package com.dataworks.crud.delete;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class SimpleDelete {
	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 100, "colfam1", "colfam2");

		util.put("forfun", 
				new String[] { "row1" }, 
				new String[] { "colfam1", "colfam2" },
				new String[] { "qual1", "qual1", "qual2", "qual2", "qual3", "qual3" }, 
				new long[] { 1, 2, 3, 4, 5, 6 },
				new String[] { "val1", "val1", "val2", "val2", "val3", "val3" });
		System.out.println("Before Delete call...");

		util.dump("forfun", new String[] { "row1" }, null, null);

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		Delete delete = new Delete(Bytes.toBytes("row1"));
		delete.setTimestamp(1);
		delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
		delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3"), 3);

		delete.addColumns(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
		delete.addColumns(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3"), 2);

		delete.addFamily(Bytes.toBytes("colfam1"));
		delete.addFamily(Bytes.toBytes("colfam1"), 3);

		tbl.delete(delete);
		System.out.println("After Delete...");
		util.dump("forfun", new String[] { "row1" }, null, null);
		util.close();
	}

}
