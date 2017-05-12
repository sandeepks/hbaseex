package com.dataworks.crud.delete;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class ListDelete {

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

		util.put("forfun", 
				new String[] { "row2" }, 
				new String[] { "colfam1", "colfam2" },
				new String[] { "qual1", "qual1", "qual2", "qual2", "qual3", "qual3" }, 
				new long[] { 1, 2, 3, 4, 5, 6 },
				new String[] { "val1", "val1", "val2", "val2", "val3", "val3" });

		util.put("forfun", 
				new String[] { "row3" }, 
				new String[] { "colfam1", "colfam2" },
				new String[] { "qual1", "qual1", "qual2", "qual2", "qual3", "qual3" }, 
				new long[] { 1, 2, 3, 4, 5, 6 },
				new String[] { "val1", "val1", "val2", "val2", "val3", "val3" });

		System.out.println("Before Delete call...");
		util.dump("forfun", new String[] { "row1", "row2", "row3" }, null, null);

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		List<Delete> deletes = new ArrayList<Delete>();

		Delete delete1 = new Delete(Bytes.toBytes("row1"));
		delete1.setTimestamp(4);
		deletes.add(delete1);

		Delete delete2 = new Delete(Bytes.toBytes("row2"));
		delete2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
		delete2.addColumns(Bytes.toBytes("colfam2"), Bytes.toBytes("qual3"), 5);
		deletes.add(delete2);

		Delete delete3 = new Delete(Bytes.toBytes("row3"));
		delete3.addFamily(Bytes.toBytes("colfam1"));
		delete3.addFamily(Bytes.toBytes("colfam2"), 3);
		deletes.add(delete3);

		tbl.delete(deletes);
		System.out.println("After Delete...");
		util.dump("forfun", new String[] { "row1", "row2", "row3" }, null, null);
		tbl.close();
		util.close();

	}

}
