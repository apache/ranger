package com.xasecure.hadoop.client;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.xasecure.hadoop.client.config.BaseClient;
import com.xasecure.hadoop.client.exceptions.HadoopException;

public class HadoopFS extends BaseClient {
	
	public HadoopFS(String dataSource) {
		super(dataSource) ;
	}
	
	public HadoopFS(String dataSource, HashMap<String,String> connectionProperties) {
		super(dataSource,connectionProperties) ;
	}
	
	
	private List<String> listFilesInternal(String baseDir, String fileMatching) {
		List<String> fileList = new ArrayList<String>() ;
		ClassLoader prevCl = Thread.currentThread().getContextClassLoader() ;
		try {
			Thread.currentThread().setContextClassLoader(getConfigHolder().getClassLoader());
			String dirPrefix = (baseDir.endsWith("/") ? baseDir : (baseDir + "/")) ;
			String filterRegEx = null;
			if (fileMatching != null && fileMatching.trim().length() > 0) {
				filterRegEx = fileMatching.trim() ;
			}
			Configuration conf = new Configuration() ;
			FileSystem fs = null ;
			try {
				fs = FileSystem.get(conf) ;
				FileStatus[] fileStats = fs.listStatus(new Path(baseDir)) ;
				if (fileStats != null) {
					for(FileStatus stat : fileStats) {
						Path path = stat.getPath() ;
						String pathComponent = path.getName() ;
						if (filterRegEx == null) {
							fileList.add(dirPrefix + pathComponent) ;
						}
						else if (FilenameUtils.wildcardMatch(pathComponent, fileMatching)) {
							fileList.add(dirPrefix + pathComponent) ;
						}
					}
				}
			}
			finally {
			}
		}
		catch(IOException ioe) {
			throw new HadoopException("Unable to get listing of files for directory [" + baseDir + "] from Hadoop environment [" + getDataSource() + "]", ioe) ;
		}
		finally {
			Thread.currentThread().setContextClassLoader(prevCl);
		}
		return fileList ;
	}

	
	public List<String> listFiles(final String baseDir, final String fileMatching) {
		PrivilegedAction<List<String>> action = new PrivilegedAction<List<String>>() {
			@Override
			public List<String> run() {
				return listFilesInternal(baseDir, fileMatching) ;
			}
			
		};
		return Subject.doAs(getLoginSubject(),action) ;
	}
	
	
	public static final void main(String[] args) {
		
		if (args.length < 2) {
			System.err.println("USAGE: java " + HadoopFS.class.getName() + " repositoryName  basedirectory  [filenameToMatch]") ;
			System.exit(1) ;
		}
		
		String repositoryName = args[0] ;
		String baseDir = args[1] ;
		String fileNameToMatch = (args.length == 2 ? null : args[2]) ;
		
		HadoopFS fs = new HadoopFS(repositoryName) ;
		List<String> fsList = fs.listFiles(baseDir, fileNameToMatch) ;
		if (fsList != null && fsList.size() > 0) {
			for(String s : fsList) {
				System.out.println(s) ;
			}
		}
		else {
			System.err.println("Unable to get file listing for [" + baseDir + (baseDir.endsWith("/") ? "" : "/") + fileNameToMatch + "]  in repository [" + repositoryName + "]") ;
		}
	}

}
