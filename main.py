from MlfReportGet import MlfReportReq, MlfReportGet
from UpdateSkuSalesMonth import UpdateSkuSalesMonth

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