package com.dataworks.crud.put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class ListBufferedPut {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1");

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));
		BufferedMutator bufferedMutator = connection.getBufferedMutator(TableName.valueOf("forfun"));
		System.out.println("WriteBuffer Size = "+bufferedMutator.getWriteBufferSize());
		List<Mutation> mutations = new ArrayList<Mutation>();
		
		Put put1 = new Put(Bytes.toBytes("row1"));
		put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
		mutations.add(put1);
		
		Put put2 = new Put(Bytes.toBytes("row2"));
		put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));
		mutations.add(put2);
		
		Put put3 = new Put(Bytes.toBytes("row3"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val3"));
		mutations.add(put3);
		
		bufferedMutator.mutate(mutations);
		
		Get get = new Get(Bytes.toBytes("row1"));
		Result beforeFlush = tbl.get(get);
		System.out.println("Result: " + beforeFlush);
		
		bufferedMutator.flush();
		
		Result afterFlush = tbl.get(get);
		System.out.println("Result: " + afterFlush);
		
		bufferedMutator.close();
		tbl.close();
		util.close();

	}

}
