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
import java.util.ArrayList;
import java.util.Map;

import java.io.*;

/*
 * Persist vectors in given file into database.
 */
public class DiagonalMatrixInv {

	private static final double EPSILON = 1e-7;

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("Usage: DiagonalMatrixInv <input_file> <output_file>");
			return;
		}

		int partitionNum = 5;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Matrix vec = null;
		try {
			vec = MatrixUtils.read(conf, new Path(args[0]));
		} catch (IOException e) {
			System.err.println("Cannot read matrix from " + args[1]);
			e.printStackTrace();
		}

		Matrix matrix = new SparseMatrix(vec.columnSize(), vec.columnSize());

		double ele = 0.0;
		for (int i = 0; i < vec.columnSize() ; ++i) {
			ele = vec.viewRow(0).get(i);
			if (Math.abs(ele) < EPSILON) {
				matrix.set(i, i, 0.0);
			} else {
				matrix.set(i, i, 1.0/ele);
			}
		}


		List<SequenceFile.Writer> writers = new ArrayList<SequenceFile.Writer>();
		try {
			for (int i = 0; i < partitionNum; ++i) {
				writers.add(SequenceFile.createWriter(fs, conf, new Path(args[1]+"/part-0000"+i), IntWritable.class, VectorWritable.class));
			}
			for (int i = 0; i < vec.columnSize(); ++i) {
					IntWritable id = new IntWritable();
					VectorWritable vector = new VectorWritable();
					id.set(i);
					vector.set(matrix.viewRow(i));
					writers.get(i%partitionNum).append(id, vector);
			}
			for (int i = 0; i < partitionNum; ++i) {
				writers.get(i).close();
			}
		} catch (IOException e) {
			System.err.println("Cannot write matrix to " + args[1]);
			e.printStackTrace();
		}
	}
}
