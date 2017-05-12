package com.dataworks.counters;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class MultiCounter {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		/* Create Table, if exists drop it and create using HBaseUtil. */
		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("counter");
		util.createTable("counter", 1, "daily", "weekly");

		Connection connection = ConnectionFactory.createConnection(configuration);
		Table tbl = connection.getTable(TableName.valueOf("counter"));
		
		Increment increment1 = new Increment(Bytes.toBytes("20170101"));
		increment1.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("clicks"), 1);
		increment1.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);
		increment1.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("clicks"),  10);
		increment1.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("hits"), 7);
		
		Map<byte[], NavigableMap<byte[], Long>> counterValue = increment1.getFamilyMapOfLongs();
		for(byte[] family: counterValue.keySet()){
			System.out.println("Increment #1 - family: " + Bytes.toString(family));
		      NavigableMap<byte[], Long> longcols = counterValue.get(family);
		      for (byte[] column : longcols.keySet()) {
		        System.out.print("  column: " + Bytes.toString(column));
		        System.out.println(" - value: " + longcols.get(column));
		      }
		}
		
		Result result1 = tbl.increment(increment1); 
	    for (Cell cell : result1.rawCells()) {
	      System.out.println("Cell: " + cell +
	        " Value: " + Bytes.toLong(cell.getValueArray(), cell.getValueOffset(),
	        cell.getValueLength())); 
	    }

	    tbl.close();
		util.close();

	}

}
