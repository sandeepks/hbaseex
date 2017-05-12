package com.dataworks.counters;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class SingleCounter {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		/* Create Table, if exists drop it and create using HBaseUtil. */
		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("counter");
		util.createTable("counter", 1, "daily");

		Connection connection = ConnectionFactory.createConnection(configuration);
		Table tbl = connection.getTable(TableName.valueOf("counter"));

		long incrementColumnValue1 = tbl.incrementColumnValue(Bytes.toBytes("20170101"), Bytes.toBytes("daily"),
				Bytes.toBytes("hits"), 1);
		long incrementColumnValue2 = tbl.incrementColumnValue(Bytes.toBytes("20170101"), Bytes.toBytes("daily"),
				Bytes.toBytes("hits"), 0);
		long incrementColumnValue3 = tbl.incrementColumnValue(Bytes.toBytes("20170101"), Bytes.toBytes("daily"),
				Bytes.toBytes("hits"), -1);

		System.out.println("cnt1: " + incrementColumnValue1 + ", current: " + incrementColumnValue2 + ", cnt3: "
				+ incrementColumnValue3);
		tbl.close();
		util.close();
	}

}
