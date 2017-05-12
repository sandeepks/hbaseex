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
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataworks.util.HbaseUtil;

public class FirstKeyOnly {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();

		HbaseUtil util = HbaseUtil.getUtil(configuration);
		util.dropTable("forfun");
		util.createTable("forfun", 1, "colfam1", "colfam2", "colfam3");
		util.fillTableRandom("forfun", 1, 30, 0, 1, 30, 0, 0, 100, 0, true, "colfam1");

		Connection connection = util.getConnection();
		Table tbl = connection.getTable(TableName.valueOf("forfun"));

		FirstKeyOnlyFilter firstKeyOnlyFilter = new FirstKeyOnlyFilter();

		Scan scan = new Scan();
		scan.setFilter(firstKeyOnlyFilter);

		ResultScanner scanner = tbl.getScanner(scan);

		for (Result result : scanner) {
			for (Cell cell : result.rawCells()) {
				System.out.println("Cell: " + cell + ", Value: "
						+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
		}

	}

}
