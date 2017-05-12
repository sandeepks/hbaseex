package com.dataworks.crud.put;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class CheckAndPut {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1");

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		Put put1 = new Put(Bytes.toBytes("row1"));
		put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));

		boolean checkAndPut1 = tbl.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
				null, put1);
		System.out.println("Put 1 applied: " + checkAndPut1);

		/* value null for non-existing column*/
		boolean checkAndPut2 = tbl.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
				null, put1);
		System.out.println("Put 1 applied: " + checkAndPut2);

		Put put2 = new Put(Bytes.toBytes("row1"));
		put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));

		/*check row, fam, qual and val, if matches put is placed*/
		boolean checkAndPut3 = tbl.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
				Bytes.toBytes("val1"), put2);
		System.out.println("Put 2 applied: " + checkAndPut3);

		Put put3 = new Put(Bytes.toBytes("row2"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val3"));

		boolean checkAndPut4 = tbl.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
				Bytes.toBytes("val2"), put3);
		System.out.println("Put 1 applied: " + checkAndPut4);

		tbl.close();
		util.close();

	}

}
