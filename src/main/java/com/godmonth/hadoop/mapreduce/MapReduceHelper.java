package com.godmonth.hadoop.mapreduce;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shenyue
 */
public class MapReduceHelper {

	private static final Logger logger = LoggerFactory.getLogger(MapReduceHelper.class);
	static {
		System.setProperty("path.separator", ":");
		if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
			try {
				trackerDistributedCacheManagerClass = Class
						.forName("org.apache.hadoop.filecache.TrackerDistributedCacheManager");
			} catch (ClassNotFoundException e) {
				trackerDistributedCacheManagerClass = null;
				logger.warn("Unable to provide Windows JAR permission fix: " + e.getMessage(), e);
			}
		}
	}

	public static void addTmpJar(String jarPath, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.getLocal(conf);
		String newJarPath = new Path(jarPath).makeQualified(fs).toString();
		logger.trace("jarpath:{}", newJarPath);
		String tmpjars = conf.get("tmpjars");
		if (tmpjars == null || tmpjars.length() == 0) {
			conf.set("tmpjars", newJarPath);
		} else {
			conf.set("tmpjars", tmpjars + "," + newJarPath);
		}
	}

	public static void copyLocalJarsToHdfs(List<File> jarFiles, String hdfsJarsDir, Configuration configuration)
			throws IOException {
		FileSystem hdfsFileSystem = FileSystem.get(configuration);
		for (File jarFile : jarFiles) {
			Path localJarPath = new Path(jarFile.toURI());
			Path hdfsJarPath = new Path(hdfsJarsDir, jarFile.getName());
			hdfsFileSystem.copyFromLocalFile(false, false, localJarPath, hdfsJarPath);
		}
	}

	public static void addHdfsJarsToDistributedCache(String hdfsJarsDir, Configuration configuration)
			throws IOException {

		Set<Path> jarPaths = collectJarPathsOnHdfs(hdfsJarsDir, configuration);
		if (!jarPaths.isEmpty()) {
			logger.info("Adding following JARs to distributed cache: {}", jarPaths);
			System.setProperty("path.separator", ":"); // due to
														// https://issues.apache.org/jira/browse/HADOOP-9123

			for (Path jarPath : jarPaths) {
				FileSystem jarPathFileSystem = jarPath.getFileSystem(configuration);
				DistributedCache.addFileToClassPath(jarPath, configuration, jarPathFileSystem);
			}
		}
	}

	private static Set<Path> collectJarPathsOnHdfs(String hdfsJarsDir, Configuration configuration) throws IOException {
		Set<Path> jarPaths = new HashSet<Path>();
		FileSystem fileSystem = FileSystem.get(configuration);
		Path jarsDirPath = new Path(hdfsJarsDir);
		if (!fileSystem.exists(jarsDirPath)) {
			throw new IllegalArgumentException("Directory '" + hdfsJarsDir + "' doesn't exist on HDFS ");
		}
		if (fileSystem.isFile(jarsDirPath)) {
			throw new IllegalArgumentException("Path '" + hdfsJarsDir + "' on HDFS is file, not directory");
		}

		FileStatus[] fileStatuses = fileSystem.listStatus(jarsDirPath);
		for (FileStatus fileStatus : fileStatuses) {
			if (!fileStatus.isDir()) {
				jarPaths.add(fileStatus.getPath());
			}
		}
		return jarPaths;
	}

	private static Class<?> trackerDistributedCacheManagerClass;

	static {
		if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
			try {
				trackerDistributedCacheManagerClass = Class
						.forName("org.apache.hadoop.filecache.TrackerDistributedCacheManager");
			} catch (ClassNotFoundException e) {
				trackerDistributedCacheManagerClass = null;
				logger.warn("Unable to provide Windows JAR permission fix: " + e.getMessage(), e);
			}
		}
	}

	public static void hackHadoopStagingOnWin() {
		// do the assignment only on Windows systems
		if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
			// 0655 = -rwxr-xr-x
			JobSubmissionFiles.JOB_DIR_PERMISSION.fromShort((short) 0650);
			JobSubmissionFiles.JOB_FILE_PERMISSION.fromShort((short) 0650);

			if (trackerDistributedCacheManagerClass != null) {
				// handle jar permissions as well
				Field field = findField(trackerDistributedCacheManagerClass, "PUBLIC_CACHE_OBJECT_PERM");
				makeAccessible(field);
				try {
					FsPermission perm = (FsPermission) field.get(null);
					perm.fromShort((short) 0650);

				} catch (IllegalAccessException e) {
					throw new RuntimeException("Error while trying to set permission on field: " + field, e);
				}
				;
			}
		}
	}

	private static Field findField(Class<?> clazz, String name) {
		Class<?> searchType = clazz;
		while (!Object.class.equals(searchType) && searchType != null) {
			Field[] fields = searchType.getDeclaredFields();
			for (Field field : fields) {
				if ((name == null || name.equals(field.getName()))) {
					return field;
				}
			}
			searchType = searchType.getSuperclass();
		}
		return null;
	}

	private static void makeAccessible(Field field) {
		if ((!Modifier.isPublic(field.getModifiers()) || !Modifier.isPublic(field.getDeclaringClass().getModifiers()) || Modifier
				.isFinal(field.getModifiers())) && !field.isAccessible()) {
			field.setAccessible(true);
		}
	}

}
