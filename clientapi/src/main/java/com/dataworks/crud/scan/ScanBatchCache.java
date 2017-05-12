package com.dataworks.crud.scan;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

import com.dataworks.util.HbaseUtil;

public class ScanBatchCache {
	private static Table tbl;

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1", "colfam2");
		util.fillFor("testtable", 1, 20, 20, "colfam1", "colfam2");

		Connection connection = util.getConnection();
		tbl = connection.getTable(TableName.valueOf("forfun"));

		scan(1, 1, false);
		scan(1, 0, false);

		tbl.close();
		util.close();

	}

	private static void scan(int cache, int batch, boolean isSmall) throws IOException {
		Scan scan = new Scan().setCaching(cache).setBatch(batch).setSmall(isSmall).setScanMetricsEnabled(true);
		ResultScanner scanner = tbl.getScanner(scan);
		int count = 0;
		for (@SuppressWarnings("unused")
		Result result : scanner) {
			count++;
		}
		scanner.close();
		ScanMetrics metrics = scan.getScanMetrics();
		System.out.println("Caching: " + cache + ", Batch: " + batch + ", Small: " + isSmall + ", Results: " + count
				+ ", RPCs: " + metrics.countOfRPCcalls);

	}

}
