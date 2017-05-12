package com.dataworks.crud.put;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class SimpleBufferedPut {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();
		
		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1");
		
		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));
		
		/*set Buffer and KV size explicitly for this table*/
		BufferedMutatorParams bmParams = new BufferedMutatorParams(TableName.valueOf("forfun"));
		bmParams.writeBufferSize(2097152);
		bmParams.maxKeyValueSize(10485760);
		
		BufferedMutator bufferedMutator = connection.getBufferedMutator(bmParams);
		
		Put put1 = new Put(Bytes.toBytes("row1"));
		put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
		bufferedMutator.mutate(put1);
		
		Put put2 = new Put(Bytes.toBytes("row2"));
		put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));
		bufferedMutator.mutate(put2);
		
		Put put3 = new Put(Bytes.toBytes("row3"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val3"));
		bufferedMutator.mutate(put3);
		
		Get get = new Get(Bytes.toBytes("row1"));
		Result beforeFlush = tbl.get(get);
		System.out.println("Result, BEFORE Flush: " + beforeFlush);
		
		bufferedMutator.flush();
		
		Result afterFlush = tbl.get(get);
		System.out.println("Result: " + afterFlush);
		
		bufferedMutator.close();
		tbl.close();
		util.close();

	}

}
