package com.dataworks.filter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class KeyOnlyFilter {
	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1", "colfam2", "colfam3");
		util.fillFor("forfun", 1, 20, 10, "colfam1", "colfam2", "colfam3");

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		Filter keyOnlyFilter = new org.apache.hadoop.hbase.filter.KeyOnlyFilter();

		Scan scan = new Scan();
		scan.setFilter(keyOnlyFilter);
		ResultScanner scanner = tbl.getScanner(scan);
		for (Result result : scanner) {
			for (Cell cell : result.rawCells()) {
				//Value part and vlen shall be absent and 0
				System.out.println("Cell: " + cell + ", Value: "
						+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
		}
		
		scanner.close();

		keyOnlyFilter = new org.apache.hadoop.hbase.filter.KeyOnlyFilter(true);

		Scan scan1 = new Scan();
		scan1.setFilter(keyOnlyFilter);
		ResultScanner scanner1 = tbl.getScanner(scan);
		for (Result result : scanner1) {
			System.out.println(result);
		}
		scanner1.close();
		
		tbl.close();
		util.close();

	}

}
