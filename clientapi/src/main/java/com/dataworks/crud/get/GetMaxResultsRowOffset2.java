package com.dataworks.crud.get;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class GetMaxResultsRowOffset2 {

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration configuration = HBaseConfiguration.create();

		/* Create Table, if exists drop it and create using HBaseUtil. */
		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 3, "colfam1", "colfam2", "colfam3");

		Connection connection = util.getConnection();
		Table table = connection.getTable(TableName.valueOf("forfun"));

		for (int version = 1; version <= 3; version++) {
			String s = "row"+version;
			Put put = new Put(Bytes.toBytes(s));
			for (int n = 1; n <= 1000; n++) {
				String num = String.format("%04d", n);
				put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual" + num), Bytes.toBytes("val" + num));
				put.addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("qual" + num), Bytes.toBytes("val" + num));
				put.addColumn(Bytes.toBytes("colfam3"), Bytes.toBytes("qual" + num), Bytes.toBytes("val" + num));
			}
			System.out.println("Writing version: " + version);
			table.put(put);
			Thread.currentThread().sleep(1000);
		}

		Get get0 = new Get(Bytes.toBytes("row1"));
		get0.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual0001"));
		get0.setMaxVersions();
		Result result0 = table.get(get0);
		CellScanner scanner0 = result0.cellScanner();
		while (scanner0.advance()) {
			System.out.println("Get 0 Cell: " + scanner0.current());
		}

		Get get1 = new Get(Bytes.toBytes("row1"));
		get1.setMaxResultsPerColumnFamily(10);
		Result result1 = table.get(get1);
		CellScanner scanner1 = result1.cellScanner();
		while (scanner1.advance()) {
			System.out.println("Get 1 Cell: " + scanner1.current());
		}

		Get get2 = new Get(Bytes.toBytes("row1"));
		get2.setMaxResultsPerColumnFamily(10);
		get2.setMaxVersions(3);
		Result result2 = table.get(get2);
		CellScanner scanner2 = result2.cellScanner();
		while (scanner2.advance()) {
			System.out.println("Get 2 Cell: " + scanner2.current());
		}
		
		table.close();
		util.close();

	}

}
