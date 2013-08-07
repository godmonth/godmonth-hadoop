package com.godmonth.hadoop.hbase;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * utf-8 only
 * 
 * @author shenyue
 */
public class HbaseValueHelper {

	public static void putString(Put put, byte[] cf, Object bean, String propertyName, boolean replace)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		String property = BeanUtils.getProperty(bean, propertyName);
		if (!replace && property == null) {
			return;
		}
		put.add(cf, Bytes.toBytes(propertyName), property != null ? Bytes.toBytes(property) : null);
	}

	public void putJsonString(ObjectMapper objectMapper, Put put, byte[] cf, Object bean, String propertyName,
			boolean replace) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException,
			JsonProcessingException {
		Object property = PropertyUtils.getProperty(bean, propertyName);
		if (!replace && property == null) {
			return;
		}
		put.add(cf, Bytes.toBytes(propertyName), objectMapper.writeValueAsBytes(property));
	}

	public static void fillString(Result result, byte[] cf, Object bean, String propertyName)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		KeyValue columnLatest = result.getColumnLatest(cf, propertyName.getBytes());
		String value = new String(columnLatest.getValue(), Charsets.UTF_8);
		PropertyUtils.setProperty(bean, propertyName, value);
	}

	public static void fillJsonString(ObjectMapper objectMapper, Class<?> valueType, Result result, byte[] cf,
			Object bean, String propertyName) throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException, JsonParseException, JsonMappingException, IOException {
		KeyValue columnLatest = result.getColumnLatest(cf, propertyName.getBytes());
		Object readValue = objectMapper.readValue(columnLatest.getValue(), valueType);
		PropertyUtils.setProperty(bean, propertyName, readValue);
	}
}
