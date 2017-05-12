package com.dataworks.crud.delete;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class RowMutation {
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

		Put put = new Put(Bytes.toBytes("row1"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), 4, Bytes.toBytes("val99"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual4"), 4, Bytes.toBytes("val100"));

		Delete delete = new Delete(Bytes.toBytes("row1"));
		delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));

		RowMutations mutation = new RowMutations(Bytes.toBytes("row1"));
		mutation.add(put);
		mutation.add(delete);

		// tbl.mutateRow(mutation);
		boolean checkAndMutate1 = tbl.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("colfam2"),
				Bytes.toBytes("qual1"), CompareFilter.CompareOp.LESS, Bytes.toBytes("val1"), mutation);
		System.out.println("Mutate 1 Status = "+checkAndMutate1);
		
		Put put2 = new Put(Bytes.toBytes("row1"));
		put2.addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("qual1"), 4, Bytes.toBytes("val2"));
		tbl.put(put2);

		boolean checkAndMutate2 = tbl.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("colfam2"),
				Bytes.toBytes("qual1"), CompareFilter.CompareOp.LESS, Bytes.toBytes("val1"), mutation);
		System.out.println("Mutate 1 Status = "+checkAndMutate2);
		
		tbl.close();
		util.dump("forfun", new String[] { "row1" }, null, null);
		util.close();
		

	}

}
