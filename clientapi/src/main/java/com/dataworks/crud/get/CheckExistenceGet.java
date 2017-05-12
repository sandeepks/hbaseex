package com.dataworks.crud.get;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class CheckExistenceGet {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1");

		List<Put> puts = new ArrayList<Put>();

		Put put1 = new Put(Bytes.toBytes("row1"));
		put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
		puts.add(put1);

		Put put2 = new Put(Bytes.toBytes("row2"));
		put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));
		puts.add(put2);

		Put put3 = new Put(Bytes.toBytes("row3"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val3"));
		puts.add(put3);

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));
		tbl.put(puts);

		Get get1 = new Get(Bytes.toBytes("row1"));
		get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
		get1.setCheckExistenceOnly(true);
		Result result1 = tbl.get(get1);
		byte[] value = result1.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));

		System.out.println("Get 1 Exists: " + result1.getExists());
		System.out.println("Get 1 Size: " + result1.size());
		System.out.println("Get 1 Value: " + Bytes.toString(value));

		Get get2 = new Get(Bytes.toBytes("row2"));
		get2.addFamily(Bytes.toBytes("colfam1"));
		get2.setCheckExistenceOnly(true);
		Result result2 = tbl.get(get2);

		System.out.println("Get 2 Exists: " + result2.getExists());
		System.out.println("Get 2 Size: " + result2.size());
		System.out.println("Get 2 result: " + result2);
		
		Get get3 = new Get(Bytes.toBytes("row2"));
	    get3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual9999")); 
	    get3.setCheckExistenceOnly(true);
	    Result result3 = tbl.get(get3);

	    System.out.println("Get 3 Exists: " + result3.getExists());
	    System.out.println("Get 3 Size: " + result3.size());
	    System.out.println("Get 3 result: " + result2);
	    
	    Get get4 = new Get(Bytes.toBytes("row2"));
	    get4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual9999"));
	    get4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
	    get4.setCheckExistenceOnly(true);
	    Result result4 = tbl.get(get4);

	    System.out.println("Get 3 Exists: " + result3.getExists());
	    System.out.println("Get 3 Size: " + result3.size());
	    System.out.println("Get 3 result: " + result4);
	    
	    tbl.close();
	    util.close();

	}

}
