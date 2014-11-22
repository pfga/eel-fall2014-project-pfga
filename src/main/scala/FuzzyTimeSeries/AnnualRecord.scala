package FuzzyTimeSeries

import Main.PFGAConstants._

/*
* This class is used to hold details of time based events.
*/
case class AnnualRecord(timeSlot: String, events: Int,
                        var fuzzySet: String = "", var flrgLH: String = "",
                        var flrgRH: String = "", var fcEvents: Int = 0) {
  override def toString = s"$timeSlot$MSE_DELIM$events$MSE_DELIM$fcEvents"


}
