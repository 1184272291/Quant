import threading
import time
from datetime import datetime
import pandas as pd
import mplfinance as mpf

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract


class IBApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.bars = []
        self.done_event = threading.Event()

    # ===== 新版 ibapi error 回调（参数必须这样写）=====
    def error(self, reqId, errorTime, errorCode, errorString, advancedOrderRejectJson=None):
        # 2104 / 2158 等是“正常连接信息”，不是错误
        print(f"[IB] reqId={reqId} code={errorCode} msg={errorString}")

    # ===== 接收历史数据 =====
    def historicalData(self, reqId, bar):
        self.bars.append({
            "Date": bar.date,
            "Open": bar.open,
            "High": bar.high,
            "Low": bar.low,
            "Close": bar.close,
            "Volume": bar.volume
        })

    def historicalDataEnd(self, reqId, start, end):
        print(f"[IB] Historical data finished: {start} -> {end}")
        self.done_event.set()

    # ===== 转成 DataFrame =====
    def get_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.bars)
        if df.empty:
            return df

        # 解析日期（兼容 YYYYMMDD / 带时间）
        def parse_date(x):
            x = str(x).strip()
            if len(x) == 8 and x.isdigit():
                return datetime.strptime(x, "%Y%m%d")
            return pd.to_datetime(x, errors="coerce")

        df["Date"] = df["Date"].apply(parse_date)
        df = df.dropna(subset=["Date"])
        df = df.sort_values("Date").set_index("Date")

        # ===== 关键修复：确保 OHLCV 全是数值 =====
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Volume 不能有 NaN
        df["Volume"] = df["Volume"].fillna(0).astype(int)

        # OHLC 有 NaN 的行直接丢掉
        df = df.dropna(subset=["Open", "High", "Low", "Close"])

        return df


def make_soxs_contract() -> Contract:
    contract = Contract()
    contract.symbol = "SOXS"
    contract.secType = "STK"     # ETF 在 IB 里仍然是 STK
    contract.exchange = "SMART"
    contract.currency = "USD"
    return contract


def main():
    app = IBApp()

    # ===== 连接 IB（Paper Trading 通常是 7497）=====
    app.connect("127.0.0.1", 7497, clientId=1)

    # 启动网络线程（run 是阻塞循环）
    threading.Thread(target=app.run, daemon=True).start()
    time.sleep(1)

    contract = make_soxs_contract()

    # ===== 请求过去 1 年的日线 =====
    app.reqHistoricalData(
        reqId=1,
        contract=contract,
        endDateTime="",
        durationStr="1 Y",
        barSizeSetting="1 day",
        whatToShow="TRADES",
        useRTH=1,
        formatDate=1,
        keepUpToDate=False,
        chartOptions=[]
    )

    # 等待数据返回
    if not app.done_event.wait(timeout=30):
        print("[WARN] 等待历史数据超时")
        app.disconnect()
        return
    df = app.get_dataframe()
    print(df.tail(5))
    app.disconnect()



if __name__ == "__main__":
    main()
