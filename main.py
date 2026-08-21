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
