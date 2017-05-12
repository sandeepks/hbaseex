package com.dataworks.crud.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class SimpleBatch {
	private final static byte[] ROW1 = Bytes.toBytes("row1");
	private final static byte[] ROW2 = Bytes.toBytes("row2");
	private final static byte[] COLFAM1 = Bytes.toBytes("colfam1");
	private final static byte[] COLFAM2 = Bytes.toBytes("colfam2");
	private final static byte[] QUAL1 = Bytes.toBytes("qual1");
	private final static byte[] QUAL2 = Bytes.toBytes("qual2");

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 100, "colfam1", "colfam2");

		util.put("forfun", 
				new String[] { "row1" }, 
				new String[] { "colfam1", "colfam2" },
				new String[] { "qual1", "qual2", "qual3" }, 
				new long[] { 1, 2, 3 },
				new String[] { "val1", "val2", "val3", });
		
		System.out.println("Before Check and Mutate call...");

		util.dump("forfun", new String[] { "row1" }, null, null);
		
		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		List<Row> operations = new ArrayList<Row>();

		Put put = new Put(ROW2);
		put.addColumn(COLFAM2, QUAL1, 4, Bytes.toBytes("val5"));
		operations.add(put);

		Get get1 = new Get(ROW1);
		get1.addColumn(COLFAM1, QUAL1);
		operations.add(get1);

		Delete delete = new Delete(ROW1);
		delete.addColumn(COLFAM1, QUAL2);
		operations.add(delete);

		Get get2 = new Get(ROW2);
		get2.addFamily(Bytes.toBytes("BOGUS"));
		operations.add(get2);

		Object[] results = new Object[operations.size()];

		try {
			tbl.batch(operations, results);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		for (int i = 0; i < results.length; i++) {
			System.out
					.println("Result[" + i + "]: type = " + results[i].getClass().getSimpleName() + "; " + results[i]);
		}
		
		tbl.close();
		util.dump("forfun", new String[] { "row1" }, null, null);
		util.close();
	}

}
