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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class RowsFilter {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1", "colfam2");
		util.fillFor("forfun", 1, 100, 100, "colfam1", "colfam2");

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"));

		Filter binaryFilter = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row-22")));
		scan.setFilter(binaryFilter);
		ResultScanner scanner1 = tbl.getScanner(scan);
		
		for(Result result:scanner1){
			System.out.println(result);
		}
		scanner1.close();
		
		Filter regexFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(".*-.5"));
		scan.setFilter(regexFilter);
		ResultScanner scanner2 = tbl.getScanner(scan);
		
		for(Result result:scanner2){
			System.out.println(result);
		}
		scanner2.close();
		
		Filter substringFilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(".5"));
		scan.setFilter(substringFilter);
		ResultScanner scanner3 = tbl.getScanner(scan);
		
		for(Result result:scanner3){
			System.out.println(result);
		}
		scanner3.close();
		
		tbl.close();
		util.close();

	}

}
