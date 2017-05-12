package com.dataworks.crud.get;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class SimpleGet {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);

		if (!util.existsTable("forfun")) {
			util.createTable("forfun", 1, "colfam1");
		}

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));
		
		Get get = new Get(Bytes.toBytes("row1"));
		get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
		Result result = tbl.get(get);
		byte[] row = result.getRow();
		System.out.println("Row = "+Bytes.toString(row));
		
		byte[] value = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
		System.out.println("Value: " + Bytes.toString(value));
		
		tbl.close();
		util.close();

	}

}
