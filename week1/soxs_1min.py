import threading
import time
import re
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract


class IBApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.bars = []
        self.done_event = threading.Event()
        self.ready_event = threading.Event()

    # 新版 ibapi 的 error 回调签名（必须这样写）
    def error(self, reqId, errorTime, errorCode, errorString, advancedOrderRejectJson=None):
        # 2104/2158 等多为“正常连接信息”，也打印出来便于你判断
        print(f"[ERROR-CB] reqId={reqId} time={errorTime} code={errorCode} msg={errorString}")

    def nextValidId(self, orderId: int):
        print(f"[READY] nextValidId={orderId} (说明已建立 API 会话)")
        self.ready_event.set()

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
        print(f"[DONE] historicalDataEnd reqId={reqId} start={start} end={end}")
        self.done_event.set()

    @staticmethod
    def _parse_ib_date(x):
        """
        兼容 ibapi 常见 date 格式：
        - 'YYYYMMDD'
        - 'YYYYMMDD  HH:MM:SS' (有时双空格)
        - 'YYYYMMDD HH:MM:SS'
        - '... US/Eastern'（末尾带时区文本）
        """
        s = str(x).strip()

        # 去掉尾部时区（例如 ' US/Eastern'）
        s = re.sub(r"\s+[A-Za-z/_+-]+$", "", s).strip()

        # 压缩多空格为 1 个
        s = re.sub(r"\s+", " ", s)

        if re.fullmatch(r"\d{8}", s):
            return datetime.strptime(s, "%Y%m%d")

        if re.fullmatch(r"\d{8} \d{2}:\d{2}:\d{2}", s):
            return datetime.strptime(s, "%Y%m%d %H:%M:%S")

        # 兜底
        return pd.to_datetime(s, errors="coerce")

    def get_df(self) -> pd.DataFrame:
        df = pd.DataFrame(self.bars)
        if df.empty:
            print("[DEBUG] self.bars is empty")
            return df

        print("[DEBUG] raw bars len:", len(df))
        print("[DEBUG] sample raw Date:", df["Date"].head(5).tolist())

        df["Date"] = df["Date"].apply(self._parse_ib_date)

        bad = df["Date"].isna().sum()
        print(f"[DEBUG] bad Date parsed rows: {bad} / {len(df)}")

        df = df.dropna(subset=["Date"]).sort_values("Date").set_index("Date")

        # 强制数值化
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # 丢掉 OHLC 异常行
        df = df.dropna(subset=["Open", "High", "Low", "Close"])

        # Volume 允许为空则填 0
        df["Volume"] = df["Volume"].fillna(0)

        print("[DEBUG] final df len:", len(df))
        return df


def make_soxs_contract() -> Contract:
    c = Contract()
    c.symbol = "SOXS"
    c.secType = "STK"      # ETF 在 IB 里仍然是 STK
    c.exchange = "SMART"
    c.currency = "USD"
    return c


def main():
    app = IBApp()

    host = "127.0.0.1"
    port = 7497      # Paper 通常 7497；Live 常见 7496（按你 TWS 设置改）
    client_id = 7    # 换一个不常用的，避免 clientId 冲突

    print(f"[CONNECT] {host}:{port} clientId={client_id}")
    app.connect(host, port, clientId=client_id)

    threading.Thread(target=app.run, daemon=True).start()

    # 等 API session ready
    if not app.ready_event.wait(timeout=15):
        print("[FATAL] 15 秒内没收到 nextValidId：大概率没连上 / API 未启用 / 端口不对")
        app.disconnect()
        return

    contract = make_soxs_contract()

    # 上周（过去 1 周）的 1 分钟数据
    req_id = 101
    print("[REQ] reqHistoricalData: duration=1 W, bar=1 min, useRTH=0")
    app.reqHistoricalData(
        reqId=req_id,
        contract=contract,
        endDateTime="",          # 以“现在”为结束点往回取
        durationStr="1 W",       # 过去 1 周
        barSizeSetting="1 min",  # 1 分钟 bar
        whatToShow="TRADES",
        useRTH=0,                # 排查阶段建议 0（包含盘前盘后）
        formatDate=1,
        keepUpToDate=False,
        chartOptions=[]
    )

    # 1分钟数据量较大，给足等待时间
    if not app.done_event.wait(timeout=180):
        print(f"[TIMEOUT] 180 秒仍未收到 historicalDataEnd。raw bars={len(app.bars)}")
        app.disconnect()
        return

    df = app.get_df()
    app.disconnect()

    if df.empty:
        print("[RESULT] DataFrame 为空。请检查上方是否有 errorCode（例如 200/162/354/10167/10168）")
        return

    print("[RESULT] df head:")
    print(df.head())
    print("[RESULT] df tail:")
    print(df.tail())

    # 画 1分钟收盘价折线（数据点很多，折线更稳）
    plt.figure()
    plt.plot(df.index, df["Close"])
    plt.title("SOXS - 1 Minute Close (Last 1 Week)")
    plt.xlabel("Time")
    plt.ylabel("Close")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
