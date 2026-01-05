# -*- coding: utf-8 -*-

from ibapi.client import EClient
from ibapi.wrapper import EWrapper

class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self,self)
        
    def error(self, reqId, errorTime, errorCode, errorString, advancedOrderRejectJson=None):
        print(f"ERROR {errorCode} | reqId={reqId} | {errorString}")

app = TradingApp()      
app.connect("127.0.0.1", 7497, clientId=1)
app.run()
    