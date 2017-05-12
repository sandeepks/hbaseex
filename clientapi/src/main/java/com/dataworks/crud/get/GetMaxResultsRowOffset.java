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

public class GetMaxResultsRowOffset {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 3, "colfam1");

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		Put put = new Put(Bytes.toBytes("row1"));
		for (int n = 1; n <= 1000; n++) {
			String num = String.format("%04d", n);
			put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual" + num), Bytes.toBytes("val" + num));
		}
		tbl.put(put);

		Get get1 = new Get(Bytes.toBytes("row1"));
		get1.setMaxResultsPerColumnFamily(10);
		Result result1 = tbl.get(get1);
		CellScanner scanner1 = result1.cellScanner();
		while (scanner1.advance()) {
			System.out.println("Get 1 Cell: " + scanner1.current());
		}

		Get get2 = new Get(Bytes.toBytes("row1"));
		get2.setRowOffsetPerColumnFamily(100);
		get2.setMaxResultsPerColumnFamily(10);
		Result result2 = tbl.get(get2);
		CellScanner scanner2 = result2.cellScanner();

		while (scanner2.advance()) {
			System.out.println("Get 2 Cell: " + scanner2.current());
		}
		tbl.close();
		util.close();
	}

}
