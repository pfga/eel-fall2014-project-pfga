package GAandFTSFramework;

/**
 * This is the data structure containing each year's record.
 *
 * @author ankitsirmorya
 */
public class AnnualRecord {

    private String year = null;
    private int visitors = 0;
    private String fuzzySet = null;
    private String flrgLH = null;
    private String flrgRH = null;
    private int fcVisitors = 0;

    public AnnualRecord() {

    }

    public AnnualRecord(String year, int visitors) {
        this.year = year;
        this.visitors = visitors;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public int getVisitors() {
        return visitors;
    }

    public void setVisitors(int visitors) {
        this.visitors = visitors;
    }

    public String getFuzzySet() {
        return fuzzySet;
    }

    public void setFuzzySet(String fuzzySet) {
        this.fuzzySet = fuzzySet;
    }

    public String getFlrgLH() {
        return flrgLH;
    }

    public void setFlrgLH(String flrgLH) {
        this.flrgLH = flrgLH;
    }

    public String getFlrgRH() {
        return flrgRH;
    }

    public void setFlrgRH(String flrgRH) {
        this.flrgRH = flrgRH;
    }

    public int getFcVisitors() {
        return fcVisitors;
    }

    public void setFcVisitors(int fcVisitors) {
        this.fcVisitors = fcVisitors;
    }

}
