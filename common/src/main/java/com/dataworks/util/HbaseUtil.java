package com.dataworks.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtil implements Closeable {
	private Configuration configuration;
	private Connection connection;
	private Admin admin;

	/**
	 * 
	 * @param configuration
	 * @throws IOException
	 */
	public HbaseUtil(Configuration configuration) throws IOException {
		super();
		this.configuration = configuration;
		this.connection = ConnectionFactory.createConnection(configuration);
		this.admin = connection.getAdmin();
	}

	public static HbaseUtil getUtil(Configuration configuration) throws IOException {
		return new HbaseUtil(configuration);
	}

	@Override
	public void close() throws IOException {
		if (null != connection) {
			connection.close();
		}
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public Connection getConnection() {
		return connection;
	}

	public boolean existsTable(String tbl) throws IOException {
		return admin.tableExists(TableName.valueOf(tbl));
	}

	public boolean existsTable(TableName tbl) throws IOException {
		return admin.tableExists(tbl);
	}

	/**
	 * 
	 * @param tbl
	 * @param colFams
	 * @throws IOException
	 */
	public void createTable(String tbl, String... colFams) throws IOException {
		createTable(TableName.valueOf(tbl), 1, null, colFams);
	}

	public void createTable(String nameSpace, String tbl, String... colFams) throws IOException {
		createTable(TableName.valueOf(nameSpace, tbl), 1, null, colFams);
	}

	/**
	 * 
	 * @param tbl
	 * @param colFams
	 * @throws IOException
	 */
	public void createTable(TableName tbl, String... colFams) throws IOException {
		createTable(tbl, 1, null, colFams);
	}

	/**
	 * 
	 * @param tbl
	 * @param maxVersions
	 * @param colFams
	 * @throws IOException
	 */
	public void createTable(String nameSpace, String tbl, int maxVersions, String... colFams) throws IOException {
		createTable(TableName.valueOf(nameSpace, tbl), maxVersions, null, colFams);
	}

	public void createTable(String tbl, int maxVersions, String... colFams) throws IOException {
		createTable(TableName.valueOf(tbl), maxVersions, null, colFams);
	}

	/**
	 * 
	 * @param tbl
	 * @param maxVersions
	 * @param colFams
	 * @throws IOException
	 */
	public void createTable(TableName tbl, int maxVersions, String... colFams) throws IOException {
		createTable(tbl, maxVersions, null, colFams);
	}

	/**
	 * 
	 * @param tbl
	 * @param splitKeys
	 * @param colFams
	 * @throws IOException
	 */
	public void createTable(String tbl, byte[][] splitKeys, String... colFams) throws IOException {
		createTable(TableName.valueOf(tbl), 1, splitKeys, colFams);
	}

	/**
	 * 
	 * @param tbl
	 * @param splitKeys
	 * @param colFams
	 * @throws IOException
	 */
	public void createTable(TableName tbl, byte[][] splitKeys, String... colFams) throws IOException {
		createTable(tbl, 1, splitKeys, colFams);
	}

	/**
	 * 
	 * @param tbl
	 * @param maxVersions
	 * @param splitKeys
	 * @param colFams
	 * @throws IOException
	 */
	private void createTable(TableName tbl, int maxVersions, byte[][] splitKeys, String[] colFams) throws IOException {
		HTableDescriptor tableDescriptor = new HTableDescriptor(tbl);

		for (String cf : colFams) {
			HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
			columnDescriptor.setMaxVersions(maxVersions);
			tableDescriptor.addFamily(columnDescriptor);
		}

		if (null != splitKeys) {
			admin.createTable(tableDescriptor, splitKeys);
		} else {
			admin.createTable(tableDescriptor);
		}

	}

	public void disableTable(String tbl) throws IOException {
		admin.disableTable(TableName.valueOf(tbl));
	}

	public void disableTable(TableName tbl) throws IOException {
		admin.disableTable(tbl);
	}

	public void dropTable(String tbl) throws IOException {
		deleteTable(TableName.valueOf(tbl));
	}

	public void dropTable(TableName tbl) throws IOException {
		deleteTable(tbl);
	}

	private void deleteTable(TableName tbl) throws IOException {
		if (existsTable(tbl)) {
			if (admin.isTableEnabled(tbl))
				disableTable(tbl);
			admin.deleteTable(tbl);
		}

	}

	public void put(String tbl, String[] rows, String[] colFams, String[] quals, long[] ts, String[] vals)
			throws IOException {
		put(TableName.valueOf(tbl), rows, colFams, quals, ts, vals);

	}

	private void put(TableName tblName, String[] rows, String[] colFams, String[] quals, long[] ts, String[] vals)
			throws IOException {
		Table tbl = getConnection().getTable(tblName);

		for (String row : rows) {
			Put put = new Put(Bytes.toBytes(row));
			for (String fam : colFams) {
				int v = 0;
				for (String qual : quals) {
					String val = vals[v < vals.length ? v : vals.length - 1];
					long t = ts[v < ts.length ? v : ts.length - 1];

					System.out.println("Adding: " + row + " " + fam + " " + qual + " " + t + " " + val);
					put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), t, Bytes.toBytes(val));
					v++;
				}
			}
			tbl.put(put);
		}
		tbl.close();
	}

	public void dump(String tbl, String[] rows, String[] colFams, String[] quals) throws IOException {
		dump(TableName.valueOf(tbl), rows, colFams, quals);
	}

	private void dump(TableName tblName, String[] rows, String[] colFams, String[] quals) throws IOException {
		Table tbl = getConnection().getTable(tblName);

		List<Get> gets = new ArrayList<Get>();

		for (String row : rows) {
			Get get = new Get(Bytes.toBytes(row));
			get.setMaxVersions();
			if (null != colFams) {
				for (String fam : colFams) {
					for (String qual : quals) {
						get.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual));
					}
				}
			}
			gets.add(get);
		}
		Result[] results = tbl.get(gets);

		for (Result result : results) {
			for (Cell cell : result.rawCells()) {
				System.out.println("Cell: " + cell + ", Value: "
						+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
		}
		tbl.close();
	}

	public void fillFor(String table, int startRow, int endRow, int numCols, String... colFams) throws IOException {
		fillFor(TableName.valueOf(table), startRow, endRow, numCols, colFams);
	}

	public void fillFor(TableName table, int startRow, int endRow, int numCols, String... colFams) throws IOException {
		fillFor(table, startRow, endRow, numCols, -1, false, colFams);
	}

	public void fillFor(String table, int startRow, int endRow, int numCols, boolean setTimestamp, String... colFams)
			throws IOException {
		fillFor(TableName.valueOf(table), startRow, endRow, numCols, -1, setTimestamp, colFams);
	}

	public void fillFor(TableName table, int startRow, int endRow, int numCols, boolean setTimestamp, String... colFams)
			throws IOException {
		fillFor(table, startRow, endRow, numCols, -1, setTimestamp, colFams);
	}

	public void fillFor(String table, int startRow, int endRow, int numCols, int pad, boolean setTimestamp,
			String... colFams) throws IOException {
		fill(TableName.valueOf(table), startRow, endRow, numCols, pad, setTimestamp, false, colFams);
	}

	public void fillFor(TableName table, int startRow, int endRow, int numCols, int pad, boolean setTimestamp,
			String... colFams) throws IOException {
		fill(table, startRow, endRow, numCols, pad, setTimestamp, false, colFams);
	}

	public void fill(String table, int startRow, int endRow, int numCols, int pad, boolean setTimestamp, boolean random,
			String... colFams) throws IOException {
		fill(TableName.valueOf(table), startRow, endRow, numCols, pad, setTimestamp, random, colFams);
	}

	public void fill(TableName table, int startRow, int endRow, int numCols, int pad, boolean setTimestamp,
			boolean random, String... colFams) throws IOException {
		Table tbl = getConnection().getTable(table);
		Random rnd = new Random();
		for (int row = startRow; row <= endRow; row++) {
			for (int col = 1; col <= numCols; col++) {
				Put put = new Put(Bytes.toBytes("row-" + padNum(row, pad)));
				for (String cf : colFams) {
					String colName = "col-" + padNum(col, pad);
					String val = "val-" + (random ? Integer.toString(rnd.nextInt(numCols))
							: padNum(row, pad) + "." + padNum(col, pad));
					if (setTimestamp) {
						put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), col, Bytes.toBytes(val));
					} else {
						put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), Bytes.toBytes(val));
					}
				}
				tbl.put(put);
			}
		}
		tbl.close();
	}

	private String padNum(int num, int pad) {
		String res = Integer.toString(num);
		if (pad > 0) {
			while (res.length() < pad) {
				res = "0" + res;
			}
		}
		return res;
	}

	public void fillTableRandom(String table, int minRow, int maxRow, int padRow, int minCol, int maxCol, int padCol,
			int minVal, int maxVal, int padVal, boolean setTimestamp, String... colfams) throws IOException {
		fillTableRandom(TableName.valueOf(table), minRow, maxRow, padRow, minCol, maxCol, padCol, minVal, maxVal,
				padVal, setTimestamp, colfams);
	}

	public void fillTableRandom(TableName table, int minRow, int maxRow, int padRow, int minCol, int maxCol, int padCol,
			int minVal, int maxVal, int padVal, boolean setTimestamp, String... colfams) throws IOException {
		Table tbl = connection.getTable(table);
		Random rnd = new Random();
		int maxRows = minRow + rnd.nextInt(maxRow - minRow);
		for (int row = 0; row < maxRows; row++) {
			int maxCols = minCol + rnd.nextInt(maxCol - minCol);
			for (int col = 0; col < maxCols; col++) {
				int rowNum = rnd.nextInt(maxRow - minRow + 1);
				Put put = new Put(Bytes.toBytes("row-" + padNum(rowNum, padRow)));
				for (String cf : colfams) {
					int colNum = rnd.nextInt(maxCol - minCol + 1);
					String colName = "col-" + padNum(colNum, padCol);
					int valNum = rnd.nextInt(maxVal - minVal + 1);
					String val = "val-" + padNum(valNum, padCol);
					if (setTimestamp) {
						put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), col, Bytes.toBytes(val));
					} else {
						put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), Bytes.toBytes(val));
					}
				}
				tbl.put(put);
			}
		}
		tbl.close();
	}

}
