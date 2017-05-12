package com.dataworks.crud.scan;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class SimpleScan {
	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1", "colfam2");
		util.fillFor("forfun", 1, 100, 100, "colfam1", "colfam2");

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		Scan scan1 = new Scan();
		ResultScanner scanner1 = tbl.getScanner(scan1);
		for (Result result : scanner1) {
			for (Cell cell : result.rawCells()) {
				System.out.println("Cell: " + cell + ", Value: "
						+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
		}
		scanner1.close();

		Scan scan2 = new Scan();
		scan2.addFamily(Bytes.toBytes("colfam1"));
		ResultScanner scanner2 = tbl.getScanner(scan2);
		for (Result result : scanner2) {
			for (Cell cell : result.rawCells()) {
				System.out.println("Cell: " + cell + ", Value: "
						+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
		}
		scanner2.close();

		Scan scan3 = new Scan();
		scan3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5"))
			 .setStartRow(Bytes.toBytes("row-10"))
			 .setStopRow(Bytes.toBytes("row-20"));
		
		ResultScanner scanner3 = tbl.getScanner(scan3);
		for (Result result : scanner3) {
			for (Cell cell : result.rawCells()) {
				System.out.println("Cell: " + cell + ", Value: "
						+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
		}
		scanner3.close();

		Scan scan4 = new Scan();
		scan4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5"))
			 .addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("col-33"))
			 .setStartRow(Bytes.toBytes("row-10"))
			 .setStopRow(Bytes.toBytes("row-20")).setReversed(true);
		
		ResultScanner scanner4 = tbl.getScanner(scan4);
		for (Result result : scanner4) {
			for (Cell cell : result.rawCells()) {
				System.out.println("Cell: " + cell + ", Value: "
						+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
		}
		scanner4.close();

		tbl.close();
		util.close();
	}

}
