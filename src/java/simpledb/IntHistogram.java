package simpledb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    int[] hist;
    int min;
    int max;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.hist = new int[buckets];
        this.min = min;
        this.max = max;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        double range = (double) (max - min) / (hist.length - 1);
        hist[(int) ((v - min) / range)]++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int totalElements = Arrays.stream(hist).sum();
        double range = (double) (max - min) / (hist.length - 1);
        int index = (int) ((v - min) / range);

        switch (op) {
            case EQUALS:
                if (index < 0 || index >= hist.length) return 0.0;
                return (double) hist[index] / totalElements;
            case GREATER_THAN:
                if (index < 0) return 1.0;
                if (index >= hist.length) return 0.0;
                return (double) Arrays.stream(hist, index + 1, hist.length).sum() / totalElements;
            case LESS_THAN:
                if (index < 0) return 0.0;
                if (index >= hist.length) return 1.0;
                return (double) Arrays.stream(hist, 0, index).sum() / totalElements;
            case LESS_THAN_OR_EQ:
                if (index < 0) return 0.0;
                if (index >= hist.length) return 1.0;
                return (double) Arrays.stream(hist, 0, index + 1).sum() / totalElements;
            case GREATER_THAN_OR_EQ:
                if (index < 0) return 1.0;
                if (index >= hist.length) return 0.0;
                return (double) Arrays.stream(hist, index, hist.length).sum() / totalElements;
            case NOT_EQUALS:
                if (index < 0 || index >= hist.length) return 1.0;
                return (double) (totalElements - hist[index]) / totalElements;
            default:
                return -1;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
