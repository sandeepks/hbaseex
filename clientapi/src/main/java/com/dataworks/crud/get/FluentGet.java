package com.dataworks.crud.get;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class FluentGet {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 5, "colfam1", "colfam2");

		util.put("forfun", 
				new String[] { "row1" }, 
				new String[] { "colfam1", "colfam2" },
				new String[] { "qual1", "qual1", "qual2", "qual2" }, 
				new long[] { 1, 2, 3, 4 },
				new String[] { "val1", "val1", "val2", "val2" });
		System.out.println("Before get call...");
		
		util.dump("forfun", new String[] { "row1" }, null, null);
		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));
		
		Get get = new Get(Bytes.toBytes("row1"))
				.setId("")
				.setMaxVersions()
				.setTimeStamp(4L)
				.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))
				.addFamily(Bytes.toBytes("colfam2"));
		Result result = tbl.get(get);
		
		System.out.println("Get Json"+get.toJSON());
		System.out.println("is families set"+get.hasFamilies());
		System.out.println("# of families set"+get.numFamilies());
		System.out.println("Isolation level"+get.getIsolationLevel());
		
		System.out.println("Get Result...");
		for (Cell cell : result.rawCells()) {
			System.out.println("Cell: " + cell + ", Value: "
					+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
		}
		
		tbl.close();
		util.close();
	}

}
