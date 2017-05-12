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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class PreFixFilter {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1", "colfam2");
		util.fillFor("forfun", 1, 10, 10, "colfam1", "colfam2");

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));
		
		Filter prefixFilter = new PrefixFilter(Bytes.toBytes("row-1"));
		
		Scan scan = new Scan();
		scan.setFilter(prefixFilter);
		ResultScanner scanner = tbl.getScanner(scan);
		
		for(Result result : scanner){
			System.out.println(result);
		}
		
		scanner.close();
		tbl.close();
		util.close();
	}

}
