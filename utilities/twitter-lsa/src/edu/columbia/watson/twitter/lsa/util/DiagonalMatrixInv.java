/**
 * @author Wei Wang
 * date: Apr 25, 2013
 */

package edu.columbia.watson.twitter.lsa.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.*;
import org.apache.mahout.math.function.*;

import java.util.List;
import java.util.Map;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.io.*;

/*
 * Persist vectors in given file into database.
 */
public class DiagonalMatrixInv {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("Usage: DiagonalMatrixInv <input_file> <output_file>");
			return;
		}

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Matrix matrix = null;
		try {
			matrix = MatrixUtils.read(conf, new Path(args[0]));
		} catch (IOException e) {
			System.err.println("Cannot read matrix from " + args[1]);
			e.printStackTrace();
		}

		Matrix inv = matrix.assign(new Inverse());

		try {
			MatrixUtils.write(new Path(args[1]), conf, inv);
		} catch (IOException e) {
			System.err.println("Cannot write matrix to " + args[1]);
			e.printStackTrace();
		}
	}
}

class Inverse implements DoubleFunction {
	static final double EPSILON = 1e-6;
	public double apply(double arg) {
		if (Math.abs(arg) < EPSILON) {
			return 0.0;
		} else {
			return 1.0/arg;
		}
	}
}
