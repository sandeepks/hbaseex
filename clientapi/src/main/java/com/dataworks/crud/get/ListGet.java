package com.dataworks.crud.get;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class ListGet {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1");

		List<Get> gets = new ArrayList<Get>();

		Get get1 = new Get(Bytes.toBytes("row1"));
		get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
		gets.add(get1);

		Get get2 = new Get(Bytes.toBytes("row2"));
		get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));
		gets.add(get2);

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		Result[] results = tbl.get(gets);

		System.out.println("One way...");
		for (Result result : results) {
			String row = Bytes.toString(result.getRow());
			System.out.println("Row: " + row);

			if (result.containsColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))) {
				byte[] value = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
				System.out.println("Value = " + Bytes.toString(value));
			}

			if (result.containsColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"))) {
				byte[] value = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));
				System.out.println("Value = " + Bytes.toString(value));
			}
		}

		System.out.println("Another way...");
		for (Result result : results) {
			for (Cell cell : result.listCells()) {
				System.out
						.println("Row: " + Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
								+ " Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}

		System.out.println("Yet Another iteration...");
		for (Result result : results) {
			System.out.println(result);
		}
		
		tbl.close();
		util.close();
	}

}
