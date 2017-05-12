package com.dataworks.crud.put;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class SimplePut {
	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		/* Create Table, if exists drop it and create using HBaseUtil. */
		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1");

		Connection connection = ConnectionFactory.createConnection(configuration);
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		Put put = new Put(Bytes.toBytes("row1"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3"), System.currentTimeMillis(),
				Bytes.toBytes("val3"));
		
		String id = String.format("Hostname: %s, App: %s", InetAddress.getLocalHost().getHostName(),
				System.getProperty("sun.java.command"));
		put.setId(id);

		tbl.put(put);

		System.out.println("Put.size: " + put.size());
		System.out.println("Put.id: " + put.getId());
		System.out.println("Put.fingerprint: " + put.getFingerprint());
		System.out.println("Put.toMap: " + put.toMap());
		System.out.println("Put.toJSON: " + put.toJSON());
		System.out.println("Put.toString: " + put.toString());
		
		tbl.close();
		connection.close();
		util.close();
	}

}
