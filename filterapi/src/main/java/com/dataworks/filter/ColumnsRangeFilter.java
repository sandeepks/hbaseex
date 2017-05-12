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
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class ColumnsRangeFilter {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1", "colfam2", "colfam3");
		util.fillFor("testtable", 1, 10, 20, "colfam1", "colfam2", "colfm3");

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		Filter columnRangeFilter = new ColumnRangeFilter(Bytes.toBytes("col-02"), true, Bytes.toBytes("col-05"), true);

		Scan scan = new Scan()
				.setStartRow(Bytes.toBytes("row-03"))
				.setStopRow(Bytes.toBytes("row-08"))
				.setFilter(columnRangeFilter);
		scan.setFilter(columnRangeFilter);
		ResultScanner scanner = tbl.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(result);
		}
		scanner.close();

		tbl.close();
		util.close();

	}

}
