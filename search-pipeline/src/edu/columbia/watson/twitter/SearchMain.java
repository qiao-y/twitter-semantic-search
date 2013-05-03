package edu.columbia.watson.twitter;

import java.util.PriorityQueue;

import edu.columbia.watson.twitter.AnswerRanking.IDCosinePair;

/**
 * Entry point of the searching pipeline
 * @author qiaoyu
 *
 */

public class SearchMain {


	public static void main(String [] args)
	{
		PriorityQueue<IDCosinePair> allCosValues = new PriorityQueue<IDCosinePair>();



		allCosValues.add(new IDCosinePair(1,1.0));
		allCosValues.add(new IDCosinePair(2,2.0));
		allCosValues.add(new IDCosinePair(3,3.0));
		if (allCosValues.size() >= 3){
			allCosValues.poll();
			allCosValues.add(new IDCosinePair(100000,100000.0));
		}

		System.out.println(allCosValues.poll().getID());
		System.out.println(allCosValues.poll().getID());
		System.out.println(allCosValues.poll().getID());

	}

}
