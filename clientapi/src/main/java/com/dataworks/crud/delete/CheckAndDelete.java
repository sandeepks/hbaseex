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

public class CheckAndDelete {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 100, "colfam1", "colfam2");

		util.put("forfun", new String[] { "row1" }, 
				new String[] { "colfam1", "colfam2" },
				new String[] { "qual1", "qual1", "qual2", "qual2", "qual3", "qual3" }, 
				new long[] { 1, 2, 3, 4, 5, 6 },
				new String[] { "val1", "val1", "val2", "val2", "val3", "val3" });

		System.out.println("Before Delete call...");
		util.dump("forfun", new String[] { "row1" }, null, null);

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		Delete delete1 = new Delete(Bytes.toBytes("row1"));
		delete1.addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("qual3"));

		boolean checkAndDelete1 = tbl.checkAndDelete(Bytes.toBytes("row1"), Bytes.toBytes("colfam2"),
				Bytes.toBytes("qual3"), null, delete1);
		System.out.println("Delete 1 successful: " + checkAndDelete1);
		
		Delete delete2 = new Delete(Bytes.toBytes("row1"));
		delete2.addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("qual3"));
		tbl.delete(delete2);
		
		util.dump("forfun", new String[] { "row1" }, null, null);
		boolean checkAndDelete2 = tbl.checkAndDelete(Bytes.toBytes("row1"), Bytes.toBytes("colfam2"),
				Bytes.toBytes("qual3"), null, delete1);
		System.out.println("Delete 2 successful: " + checkAndDelete2);

	}

}
