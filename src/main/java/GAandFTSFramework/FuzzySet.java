package GAandFTSFramework;

import java.util.*;

/**
 * This class contains all the Fuzzy time Series Calculation logic
 *
 * @author ankit
 */
public class FuzzySet {

    public static List<AnnualRecord> annualRecords = new ArrayList<AnnualRecord>();
    public static int chromosome[];
    public static HashMap<String, String> arMap = new HashMap<String, String>();
    public static HashMap<String, Integer> discourseMap = new HashMap<String, Integer>();
    private int order;

    /**
     * Constructor to initialize the FuzzySet DS and the Discourse map.
     *
     * @param chromosome
     * @param order
     * @param incAnnualRecords
     */
    public FuzzySet(int chromosome[], int order,
                    List<AnnualRecord> incAnnualRecords) {
        FuzzySet.annualRecords = incAnnualRecords;
        FuzzySet.chromosome = chromosome;
        this.order = order;
        arMap = new HashMap<String, String>();
        discourseMap = new HashMap<String, Integer>();
        initializeFuzzySet();
        initializeFLRG(order);
        initializeFLRGMap();
        initializeDiscourseMap();
    }

    /**
     * Initial creation of the values.
     */
    public static void initializeFuzzySet() {
        for (AnnualRecord rec : annualRecords) {
            rec.setFuzzySet("A"
                    + (ceilSearch(chromosome, 0, chromosome.length,
                    rec.getVisitors()) + 1));
        }
    }

    /**
     * This method performs the ceilSearch for a value x.
     *
     * @param arr
     * @param low
     * @param high
     * @param x
     * @return
     */
    public static int ceilSearch(int arr[], int low, int high, int x) {

        int mid;

		/* get the index of middle element of arr[low..high] */
        mid = (low + high) / 2; /* low + (high - low)/2 */

		/* If x is same as middle element, then return mid */
        if (arr[mid] == x)
            return mid;

		/*
         * If x is greater than arr[mid], then either arr[mid + 1] is ceiling of
		 * x or ceiling lies in arr[mid+1...high]
		 */
        else if (arr[mid] < x) {
            if (mid + 1 <= high && x <= arr[mid + 1])
                return mid;
            else
                return ceilSearch(arr, mid + 1, high, x);
        }

		/*
         * If x is smaller than arr[mid], then either arr[mid] is ceiling of x
		 * or ceiling lies in arr[mid-1...high]
		 */
        else {
            if (mid - 1 >= low && x > arr[mid - 1])
                return mid - 1;
            else
                return ceilSearch(arr, low, mid - 1, x);
        }

    }

    /**
     * Initializing the FLRG DS based on the order.
     *
     * @param order
     */
    public static void initializeFLRG(int order) {
        for (int i = order - 1; i < annualRecords.size(); i++) {
            String lfrg = Utility.EMPTY_STRING;
            for (int j = i - order; j < i; j++) {
                if (j == -1) {
                    lfrg = Utility.hash + Utility.comma;
                } else
                    lfrg += annualRecords.get(j).getFuzzySet() + Utility.comma;
            }
            lfrg = lfrg.substring(0, lfrg.length() - 1);
            annualRecords.get(i).setFlrgLH(lfrg);
        }
    }

    /**
     * Initializing the FLRG map.
     */
    public static void initializeFLRGMap() {
        String rHVal = null;
        for (AnnualRecord rec : annualRecords) {
            if (rec.getFlrgLH() != "" && rec.getFlrgLH() != null) {
                if (arMap.containsKey(rec.getFlrgLH())) {
                    rHVal = arMap.get(rec.getFlrgLH());
                    if (!Utility.commaSeperatedToList(rHVal).contains(
                            rec.getFuzzySet())) {
                        rHVal += Utility.comma + rec.getFuzzySet();
                        arMap.put(rec.getFlrgLH(), rHVal);
                    }
                } else {
                    arMap.put(rec.getFlrgLH(), rec.getFuzzySet());
                }
            }
        }
        // Initialising the right hand flrg of each record
        for (AnnualRecord rec : annualRecords) {

            rec.setFlrgRH(arMap.get(rec.getFlrgLH()));
        }
        Iterator it = arMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry) it.next();

            it.remove();
        }
    }

    /**
     * It initializes the discourse map
     */
    public static void initializeDiscourseMap() {
        for (int i = 0; i < chromosome.length - 1; i++) {
            discourseMap.put("A" + (i + 1),
                    (int) Math.ceil((chromosome[i] + chromosome[i + 1]) / 2));
        }
    }

    /**
     * It computes teh forecasted MSE.
     *
     * @return
     */
    public double computeForcastedValue() {
        for (AnnualRecord rec : annualRecords) {
            if (Utility.isEmpty(rec.getFlrgRH()))
                continue;
            int sum = 0, n = 0;
            for (String s : Utility.commaSeperatedToList(rec.getFlrgRH())) {
                sum += discourseMap.get(s);
                n++;
            }
            rec.setFcVisitors(sum / n);
        }
        double fcSum = 0.0;
        int n = 0;
        for (AnnualRecord rec : annualRecords) {
            if (rec.getFcVisitors() != 0) {
                fcSum += Math.pow(rec.getFcVisitors() - rec.getVisitors(), 2);
                n++;
            }

        }
        //Calculating the MSE
        return Math.pow(fcSum / n, 0.5);
    }
}