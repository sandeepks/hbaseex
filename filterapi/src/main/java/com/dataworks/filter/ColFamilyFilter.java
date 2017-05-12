package com.dataworks.filter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class ColFamilyFilter {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1", "colfam2", "colfam3", "colfam4");
		util.fillFor("testtable", 1, 10, 2, "colfam1", "colfam2", "colfam3", "colfam4");

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"));
		
		Filter filter1 = new FamilyFilter(CompareOp.LESS, new BinaryComparator(Bytes.toBytes("colfam2")));
		scan.setFilter(filter1);
		ResultScanner scanner = tbl.getScanner(scan);
		for(Result result : scanner){
			System.out.println(result);
		}
		scanner.close();
		
		tbl.close();
		util.close();
	}

}
