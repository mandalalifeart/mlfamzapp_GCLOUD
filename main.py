from MlfReportGet import MlfReportReq, MlfReportGet


def UpdateSkuSalesMonth(request):
    # lazy import: this module reads POCKETBASE_* env vars at import time,
    # which are only set for this function's deployment, not the others
    from UpdateSkuSalesMonth import UpdateSkuSalesMonth as _impl
    return _impl(request)


def GetSkuSalesByDateRange(request):
    from GetSalesDepartmentReport import GetSkuSalesByDateRange as _impl
    return _impl(request)


def GetSalesDepartmentReport(request):
    from GetSalesDepartmentReport import GetSalesDepartmentReport as _impl
    return _impl(request)


def GetMarketplaceSalesSummary(request):
    from GetMarketplaceSalesSummary import GetMarketplaceSalesSummary as _impl
    return _impl(request)


def GetNextOrderData(request):
    from GetNextOrderData import GetNextOrderData as _impl
    return _impl(request)


def UpdateNextOrderField(request):
    from UpdateNextOrderField import UpdateNextOrderField as _impl
    return _impl(request)


def UpdateNextShipmentDate(request):
    from NextOrderSettings import UpdateNextShipmentDate as _impl
    return _impl(request)


def GetUkInventory(request):
    from GetUkInventory import GetUkInventory as _impl
    return _impl(request)


def GetDeInventory(request):
    from GetDeInventory import GetDeInventory as _impl
    return _impl(request)


def AssignSkuGroup(request):
    from AssignSkuGroup import AssignSkuGroup as _impl
    return _impl(request)


def SetEndOfLife(request):
    from SetEndOfLife import SetEndOfLife as _impl
    return _impl(request)


def GetProductDetail(request):
    from GetProductDetail import GetProductDetail as _impl
    return _impl(request)


def GetDaysOfSupplyBySku(request):
    from GetDaysOfSupplyBySku import GetDaysOfSupplyBySku as _impl
    return _impl(request)


def GetAwdInventory(request):
    from GetAwdInventory import GetAwdInventory as _impl
    return _impl(request)


def UpdateUsaInventory(request):
    from UpdateUsaInventory import UpdateUsaInventory as _impl
    return _impl(request)


def AdsOAuthCallback(request):
    from AdsAuth import AdsOAuthCallback as _impl
    return _impl(request)


def GetAdsConnectionStatus(request):
    from AdsAuth import GetAdsConnectionStatus as _impl
    return _impl(request)


def UpdateAdsCampaignStats(request):
    from AdsReporting import UpdateAdsCampaignStats as _impl
    return _impl(request)


def GetAdsAccountSummary(request):
    from AdsReporting import GetAdsAccountSummary as _impl
    return _impl(request)


def GetAdsCampaignStats(request):
    from AdsReporting import GetAdsCampaignStats as _impl
    return _impl(request)


def GetAdsPortfolios(request):
    from AdsReporting import GetAdsPortfolios as _impl
    return _impl(request)


def UpdateAdsChangeHistory(request):
    from AdsChangeHistory import UpdateAdsChangeHistory as _impl
    return _impl(request)


def GetAdsChangeHistory(request):
    from AdsChangeHistory import GetAdsChangeHistory as _impl
    return _impl(request)


def UpdateAdsKeywordStats(request):
    from AdsKeywordReporting import UpdateAdsKeywordStats as _impl
    return _impl(request)


def GetAdsKeywordStats(request):
    from AdsKeywordReporting import GetAdsKeywordStats as _impl
    return _impl(request)


def UpdateAdsSearchTermStats(request):
    from AdsSearchTermReporting import UpdateAdsSearchTermStats as _impl
    return _impl(request)


def GetAdsSearchTermStats(request):
    from AdsSearchTermReporting import GetAdsSearchTermStats as _impl
    return _impl(request)


def EtsyOAuthStart(request):
    from EtsyAuth import EtsyOAuthStart as _impl
    return _impl(request)


def EtsyOAuthCallback(request):
    from EtsyAuth import EtsyOAuthCallback as _impl
    return _impl(request)


def GetEtsyConnectionStatus(request):
    from EtsyAuth import GetEtsyConnectionStatus as _impl
    return _impl(request)


def UpdateEtsyListings(request):
    from EtsyListings import UpdateEtsyListings as _impl
    return _impl(request)


def GetEtsyListings(request):
    from EtsyListings import GetEtsyListings as _impl
    return _impl(request)


def GetEtsyListingDetail(request):
    from EtsyListings import GetEtsyListingDetail as _impl
    return _impl(request)


def UpdateEtsyListingContent(request):
    from EtsyListings import UpdateEtsyListingContent as _impl
    return _impl(request)


def CreateEtsyListing(request):
    from EtsyListingCreate import CreateEtsyListing as _impl
    return _impl(request)


def MonitorEtsyExperiments(request):
    from EtsyListingExperiments import MonitorEtsyExperiments as _impl
    return _impl(request)


def StartEtsyExperiment(request):
    from EtsyListingExperiments import StartEtsyExperiment as _impl
    return _impl(request)


def GetAmazonListingItem(request):
    from AmazonListingOps import GetAmazonListingItem as _impl
    return _impl(request)


def DeleteAmazonListingItem(request):
    from AmazonListingOps import DeleteAmazonListingItem as _impl
    return _impl(request)


def ProcessAmazonRelistQueue(request):
    from AmazonListingOps import ProcessAmazonRelistQueue as _impl
    return _impl(request)


def ProbeAdsReportColumns(request):
    from AdsColumnProbe import ProbeAdsReportColumns as _impl
    return _impl(request)


def MarkEtsyOrderInProgress(request):
    from EtsyMcfFulfillment import MarkEtsyOrderInProgress as _impl
    return _impl(request)


def DiagnoseEtsyOrders(request):
    from EtsyOrders import DiagnoseEtsyOrders as _impl
    return _impl(request)


def UpdateEtsyOrders(request):
    from EtsyOrders import UpdateEtsyOrders as _impl
    return _impl(request)


def GetEtsyOrders(request):
    from EtsyOrders import GetEtsyOrders as _impl
    return _impl(request)


def RunEtsyMcfFulfillment(request):
    from EtsyMcfFulfillment import RunEtsyMcfFulfillment as _impl
    return _impl(request)


def RunEtsyMcfFulfillmentWet(request):
    from EtsyMcfFulfillment import RunEtsyMcfFulfillmentWet as _impl
    return _impl(request)


def CheckMcfAccess(request):
    from EtsyMcfFulfillment import CheckMcfAccess as _impl
    return _impl(request)


def GetMcfFulfillmentPreview(request):
    from EtsyMcfFulfillment import GetMcfFulfillmentPreview as _impl
    return _impl(request)


def GetMcfOrderById(request):
    from EtsyMcfFulfillment import GetMcfOrderById as _impl
    return _impl(request)


def PatchAmazonListingAttribute(request):
    from AmazonListingOps import PatchAmazonListingAttribute as _impl
    return _impl(request)


def ProbeEuSellerId(request):
    from AmazonListingOps import ProbeEuSellerId as _impl
    return _impl(request)


def AuditAmazonListings(request):
    from AmazonListingOps import AuditAmazonListings as _impl
    return _impl(request)


def GetScheduledJobs(request):
    from JobsDashboard import GetScheduledJobs as _impl
    return _impl(request)


def UpdateScheduledJob(request):
    from JobsDashboard import UpdateScheduledJob as _impl
    return _impl(request)


def GetJobRunsLog(request):
    from JobsDashboard import GetJobRunsLog as _impl
    return _impl(request)


def GetPocketBaseCollections(request):
    from JobsDashboard import GetPocketBaseCollections as _impl
    return _impl(request)


def CreateMcfOrderForReceipt(request):
    from EtsyMcfFulfillment import CreateMcfOrderForReceipt as _impl
    return _impl(request)


def UpdateEtsyTrackingFromAmazon(request):
    from EtsyMcfFulfillment import UpdateEtsyTrackingFromAmazon as _impl
    return _impl(request)


def UpdateAmazonFinances(request):
    from AmazonFinances import UpdateAmazonFinances as _impl
    return _impl(request)


def GetPaymentsExpensesSummary(request):
    from AmazonFinances import GetPaymentsExpensesSummary as _impl
    return _impl(request)


def UpdateAmazonReturns(request):
    from AmazonReturns import UpdateAmazonReturns as _impl
    return _impl(request)


def GetReturnStats(request):
    from AmazonReturns import GetReturnStats as _impl
    return _impl(request)


def GetReturnStatsByMonth(request):
    from AmazonReturns import GetReturnStatsByMonth as _impl
    return _impl(request)


def GetPromotionSales(request):
    from GetPromotionSales import GetPromotionSales as _impl
    return _impl(request)


def GetPromotionNames(request):
    from GetPromotionSales import GetPromotionNames as _impl
    return _impl(request)


def GetReturnStatsByAsin(request):
    from AmazonReturns import GetReturnStatsByAsin as _impl
    return _impl(request)


def UpdateWiseFinances(request):
    from WiseFinances import UpdateWiseFinances as _impl
    return _impl(request)


def GetWiseFinancesSummary(request):
    from WiseFinances import GetWiseFinancesSummary as _impl
    return _impl(request)


def GetWiseLedger(request):
    from WiseLedger import GetWiseLedger as _impl
    return _impl(request)


def ImportWiseStatement(request):
    from WiseLedger import ImportWiseStatement as _impl
    return _impl(request)


def SetWiseClassificationRule(request):
    from WiseLedger import SetWiseClassificationRule as _impl
    return _impl(request)


def UploadWiseInvoice(request):
    from WiseLedger import UploadWiseInvoice as _impl
    return _impl(request)


def DeleteWiseDocument(request):
    from WiseLedger import DeleteWiseDocument as _impl
    return _impl(request)


def GetWiseDocument(request):
    from WiseLedger import GetWiseDocument as _impl
    return _impl(request)


def AutoMatchWiseInvoice(request):
    from WiseLedger import AutoMatchWiseInvoice as _impl
    return _impl(request)


def GetWiseSupplierBalances(request):
    from WiseLedger import GetWiseSupplierBalances as _impl
    return _impl(request)


def SetWiseSupplierOpening(request):
    from WiseLedger import SetWiseSupplierOpening as _impl
    return _impl(request)


def SetWiseDocumentCoverage(request):
    from WiseLedger import SetWiseDocumentCoverage as _impl
    return _impl(request)


def VatReportSummary(request):
    from VatReport import VatReportSummary as _impl
    return _impl(request)


def UploadVatReport(request):
    from VatReport import UploadVatReport as _impl
    return _impl(request)


def EeKmdXml(request):
    from VatReport import EeKmdXml as _impl
    return _impl(request)


def OssReturnCsv(request):
    from VatReport import OssReturnCsv as _impl
    return _impl(request)


def EeKmdInvoicesXlsx(request):
    from VatReport import EeKmdInvoicesXlsx as _impl
    return _impl(request)

try:
    from Orders import orders_mlf
except Exception:  # pragma: no cover
    orders_mlf = None


def wootry(request):
    if orders_mlf is None:
        return ("Orders module not available", 500)
    result = orders_mlf(0)
    return result, 200


def wootry1(request):
    if orders_mlf is None:
        return ("Orders module not available", 500)
    result = orders_mlf(1)
    return result, 200


def RunBidOptimizerDryRun(request):
    from AdsBidOptimizer import RunBidOptimizerDryRun as _impl
    return _impl(request)


def ApplyBidChange(request):
    from AdsBidWriter import ApplyBidChange as _impl
    return _impl(request)


def DisableBidTarget(request):
    from AdsBidWriter import DisableBidTarget as _impl
    return _impl(request)


def PauseProductAd(request):
    from AdsBidWriter import PauseProductAd as _impl
    return _impl(request)


def EnableProductAd(request):
    from AdsBidWriter import EnableProductAd as _impl
    return _impl(request)


def GetBidRuleProfiles(request):
    from AdsBidRuleProfiles import GetBidRuleProfiles as _impl
    return _impl(request)


def SaveBidRuleProfile(request):
    from AdsBidRuleProfiles import SaveBidRuleProfile as _impl
    return _impl(request)


def DeleteBidRuleProfile(request):
    from AdsBidRuleProfiles import DeleteBidRuleProfile as _impl
    return _impl(request)


def GetBidChangePerformance(request):
    from AdsBidWriter import GetBidChangePerformance as _impl
    return _impl(request)


def GetBidChangeLog(request):
    from AdsBidWriter import GetBidChangeLog as _impl
    return _impl(request)


def UpdateAdsAdvertisedProductStats(request):
    from AdsAdvertisedProductReporting import UpdateAdsAdvertisedProductStats as _impl
    return _impl(request)


def GetAdsAdvertisedProductStats(request):
    from AdsAdvertisedProductReporting import GetAdsAdvertisedProductStats as _impl
    return _impl(request)


def UpdateCountryPpcDaily(request):
    from UpdateCountryPpcDaily import UpdateCountryPpcDaily as _impl
    return _impl(request)


def SendDailyAdsDigest(request):
    from DailyAdsPerformanceDigest import SendDailyAdsDigest as _impl
    return _impl(request)


def SendWeeklyReturnRateDigest(request):
    from AmazonReturnRateMonitor import SendWeeklyReturnRateDigest as _impl
    return _impl(request)


def RunWeeklyUsaInventorySync(request):
    from WeeklyUsaInventorySync import RunWeeklyUsaInventorySync as _impl
    return _impl(request)


def UpdateEuInventory(request):
    from UpdateEuInventory import UpdateEuInventory as _impl
    return _impl(request)


def RunWeeklyEuInventorySync(request):
    from WeeklyEuInventorySync import RunWeeklyEuInventorySync as _impl
    return _impl(request)


def SendRecentSalesDigest(request):
    from AmazonRecentSalesDigest import SendRecentSalesDigest as _impl
    return _impl(request)


def GetCountryPpcDaily(request):
    from UpdateCountryPpcDaily import GetCountryPpcDaily as _impl
    return _impl(request)


def UpdateAsinAnalytics(request):
    from AsinAnalytics import UpdateAsinAnalytics as _impl
    return _impl(request)


def GetAsinAnalyticsList(request):
    from AsinAnalytics import GetAsinAnalyticsList as _impl
    return _impl(request)


def GetAsinAnalytics(request):
    from AsinAnalytics import GetAsinAnalytics as _impl
    return _impl(request)


def RunAdsStockGuard(request):
    from AdsStockGuard import RunAdsStockGuard as _impl
    return _impl(request)


def SendCampaignWatchDigest(request):
    from CampaignWatchDigest import SendCampaignWatchDigest as _impl
    return _impl(request)


def SendWeeklyBidChangeSummary(request):
    from WeeklyBidChangeSummary import SendWeeklyBidChangeSummary as _impl
    return _impl(request)


def UpdateStockDays(request):
    from StockDays import UpdateStockDays as _impl
    return _impl(request)


def SendArcherWeekly(request):
    from ArcherAffiliates import SendArcherWeekly as _impl
    return _impl(request)


def GetArcherDashboard(request):
    from ArcherAffiliates import GetArcherDashboard as _impl
    return _impl(request)


def SetVatManualEntry(request):
    from VatReport import SetVatManualEntry as _impl
    return _impl(request)


def SetWiseNoDocument(request):
    from WiseLedger import SetWiseNoDocument as _impl
    return _impl(request)


def SendInventoryWarning(request):
    from InventoryWarning import SendInventoryWarning as _impl
    return _impl(request)


def AddNextOrderToMalaniOrder(request):
    from UpdateNextOrderField import AddNextOrderToMalaniOrder as _impl
    return _impl(request)


def GetPublicProductCard(request):
    from PublicProductCard import GetPublicProductCard as _impl
    return _impl(request)


def SendWeeklyChangesReview(request):
    from WeeklyChangesReview import SendWeeklyChangesReview as _impl
    return _impl(request)


def SendFbaShipmentsWeekly(request):
    from FbaShipmentsWeekly import SendFbaShipmentsWeekly as _impl
    return _impl(request)


def UpdateFbaShipments(request):
    from FbaShipmentsWeekly import UpdateFbaShipments as _impl
    return _impl(request)


def GetFbaShipments(request):
    from FbaShipmentsWeekly import GetFbaShipments as _impl
    return _impl(request)


def GetAdsReviews(request):
    from AdsReviews import GetAdsReviews as _impl
    return _impl(request)


def SendOffAmazonWeekly(request):
    from OffAmazonWeekly import SendOffAmazonWeekly as _impl
    return _impl(request)


def SendPriceCheckDaily(request):
    from PriceCheckDaily import SendPriceCheckDaily as _impl
    return _impl(request)


def SendMondayDigest(request):
    from MondayDigest import SendMondayDigest as _impl
    return _impl(request)


def SendAccountHealthWeekly(request):
    from WeeklyChecks import SendAccountHealthWeekly as _impl
    return _impl(request)


def SendListingHealthWeekly(request):
    from WeeklyChecks import SendListingHealthWeekly as _impl
    return _impl(request)


def SendCouponHealthWeekly(request):
    from WeeklyChecks import SendCouponHealthWeekly as _impl
    return _impl(request)


def SendOrganicRankWeekly(request):
    from WeeklyChecks import SendOrganicRankWeekly as _impl
    return _impl(request)


def SendSearchTermHarvest(request):
    from WeeklyChecks import SendSearchTermHarvest as _impl
    return _impl(request)


def SendMonthlyProfit(request):
    from MonthlyChecks import SendMonthlyProfit as _impl
    return _impl(request)


def SendReimbursementCheck(request):
    from MonthlyChecks import SendReimbursementCheck as _impl
    return _impl(request)


def SendStorageCheck(request):
    from MonthlyChecks import SendStorageCheck as _impl
    return _impl(request)


def SendAccountingCheck(request):
    from MonthlyChecks import SendAccountingCheck as _impl
    return _impl(request)


def SendCompetitorPriceCheck(request):
    from MonthlyChecks import SendCompetitorPriceCheck as _impl
    return _impl(request)


def SendContentGapCheck(request):
    from MonthlyChecks import SendContentGapCheck as _impl
    return _impl(request)
