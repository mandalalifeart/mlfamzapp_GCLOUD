from MlfReportGet import MlfReportReq, MlfReportGet


def UpdateSkuSalesMonth(request):
    # lazy import: this module reads POCKETBASE_* env vars at import time,
    # which are only set for this function's deployment, not the others
    from UpdateSkuSalesMonth import UpdateSkuSalesMonth as _impl
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


def GetUkInventory(request):
    from GetUkInventory import GetUkInventory as _impl
    return _impl(request)


def GetDeInventory(request):
    from GetDeInventory import GetDeInventory as _impl
    return _impl(request)


def AssignSkuGroup(request):
    from AssignSkuGroup import AssignSkuGroup as _impl
    return _impl(request)


def GetProductDetail(request):
    from GetProductDetail import GetProductDetail as _impl
    return _impl(request)


def GetAwdInventory(request):
    from GetAwdInventory import GetAwdInventory as _impl
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


def CheckMcfAccess(request):
    from EtsyMcfFulfillment import CheckMcfAccess as _impl
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
