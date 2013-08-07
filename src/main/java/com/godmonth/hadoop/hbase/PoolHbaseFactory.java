package com.godmonth.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.springframework.beans.factory.DisposableBean;

public class PoolHbaseFactory implements HTableInterfaceFactory, DisposableBean {
	private HTablePool hTablePool;

	@Override
	public void destroy() throws Exception {
		hTablePool.close();
	}

	@Override
	public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
		return hTablePool.getTable(tableName);
	}

	@Override
	public void releaseHTableInterface(HTableInterface table) throws IOException {
		table.close();
	}

	public void sethTablePool(HTablePool hTablePool) {
		this.hTablePool = hTablePool;
	}

}
