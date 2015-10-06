package fetcher.moneypenny.fetcher

/**
 * Created by vivek on 06/04/15.
 */
object FinancialType extends Enumeration {
  type FinancialType = Value
  val BalanceSheet = Value("Balance Sheet")
  val PnL = Value("Profit & Loss")
  val QuarterlyResults = Value("Quarterly Results")
  val HalfYearlyResults = Value("Half Yearly Results")
  val NineMonthlyResults = Value("Nine Monthly Results")
  val YearlyResults = Value("Yearly Results")
  val CashFlow = Value("Cash Flow")
  val Ratios = Value("Ratios")
  val CapitalStructure = Value("Capital Structure")
}
