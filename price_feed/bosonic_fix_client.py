import quickfix as fix
import quickfix.Settings as Settings
import time
import sys
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('BosticFIXClient')

class BosonicApplication(fix.Application):
    def __init__(self):
        super().__init__()
        self.logger = logger
        self.session_id = None
        self.logged_on = False

    def onCreate(self, sessionID):
        self.logger.info(f"Session created: {sessionID}")
        self.session_id = sessionID
        return

    def onLogon(self, sessionID):
        self.logger.info(f"Logged on: {sessionID}")
        self.logged_on = True
        return

    def onLogout(self, sessionID):
        self.logger.info(f"Logged out: {sessionID}")
        self.logged_on = False
        return

    def toAdmin(self, message, sessionID):
        msgType = fix.MsgType()
        message.getHeader().getField(msgType)
        
        # Add user credentials to Logon message
        if msgType.getValue() == fix.MsgType_Logon:
            self.logger.info("Sending Logon message")
            # Uncomment and add your username/password if required
            # message.setField(fix.Username("YOUR_USERNAME"))
            # message.setField(fix.Password("YOUR_PASSWORD"))
        
        self.logger.info(f"Sending admin message: {message}")
        return

    def fromAdmin(self, message, sessionID):
        self.logger.info(f"Received admin message: {message}")
        return

    def toApp(self, message, sessionID):
        self.logger.info(f"Sending application message: {message}")
        return

    def fromApp(self, message, sessionID):
        self.logger.info(f"Received application message: {message}")
        
        # Process different message types here
        msgType = fix.MsgType()
        message.getHeader().getField(msgType)
        
        if msgType.getValue() == fix.MsgType_MarketDataSnapshotFullRefresh:
            self.process_market_data(message)
        elif msgType.getValue() == fix.MsgType_ExecutionReport:
            self.process_execution_report(message)
        return

    def process_market_data(self, message):
        """Process MarketData messages"""
        self.logger.info("Processing Market Data")
        
        # Extract symbol
        symbol = fix.Symbol()
        message.getField(symbol)
        
        # Number of MD entries
        no_md_entries = fix.NoMDEntries()
        message.getField(no_md_entries)
        
        self.logger.info(f"Symbol: {symbol.getValue()}, Entries: {no_md_entries.getValue()}")
        
        # Process each MD entry
        for i in range(no_md_entries.getValue()):
            group = fix.Group(268, 269)
            message.getGroup(i+1, group)
            
            md_entry_type = fix.MDEntryType()
            group.getField(md_entry_type)
            
            md_entry_px = fix.MDEntryPx()
            group.getField(md_entry_px)
            
            entry_type = "Bid" if md_entry_type.getValue() == '0' else "Offer" if md_entry_type.getValue() == '1' else md_entry_type.getValue()
            self.logger.info(f"Entry {i+1}: Type={entry_type}, Price={md_entry_px.getValue()}")

    def process_execution_report(self, message):
        """Process ExecutionReport messages"""
        self.logger.info("Processing Execution Report")
        
        # Extract key fields
        order_id = fix.OrderID()
        message.getField(order_id)
        
        exec_type = fix.ExecType()
        message.getField(exec_type)
        
        ord_status = fix.OrdStatus()
        message.getField(ord_status)
        
        self.logger.info(f"Order ID: {order_id.getValue()}, ExecType: {exec_type.getValue()}, Status: {ord_status.getValue()}")

    def send_market_data_request(self, symbol="EUR/USD"):
        """Send a market data request"""
        if not self.logged_on:
            self.logger.error("Not logged on, cannot send market data request")
            return False
            
        message = fix.Message()
        header = message.getHeader()
        header.setField(fix.MsgType(fix.MsgType_MarketDataRequest))
        
        # Set MDReqID - must be unique
        message.setField(fix.MDReqID(f"MDR-{int(time.time())}"))
        
        # Subscribe to snapshot
        message.setField(fix.SubscriptionRequestType('1'))  # 1 = Subscribe
        
        # Market depth
        message.setField(fix.MarketDepth(0))  # 0 = Full book
        
        # Update type
        message.setField(fix.MDUpdateType(0))  # 0 = Full refresh
        
        # Entry types
        no_md_entry_types = fix.NoMDEntryTypes(2)
        message.setField(no_md_entry_types)
        
        # Add bid and offer entry types
        group1 = fix.Group(267, 269)
        group1.setField(fix.MDEntryType('0'))  # 0 = Bid
        message.addGroup(group1)
        
        group2 = fix.Group(267, 269)
        group2.setField(fix.MDEntryType('1'))  # 1 = Offer
        message.addGroup(group2)
        
        # Add symbol
        no_related_sym = fix.NoRelatedSym(1)
        message.setField(no_related_sym)
        
        sym_group = fix.Group(146, 55)
        sym_group.setField(fix.Symbol(symbol))
        message.addGroup(sym_group)
        
        return fix.Session.sendToTarget(message, self.session_id)

    def send_new_order_single(self, symbol="EUR/USD", side="BUY", order_qty=1000000, price=1.1200):
        """Send a new order single request"""
        if not self.logged_on:
            self.logger.error("Not logged on, cannot send order")
            return False
            
        message = fix.Message()
        header = message.getHeader()
        header.setField(fix.MsgType(fix.MsgType_NewOrderSingle))
        
        # ClOrdID - must be unique
        message.setField(fix.ClOrdID(f"ORD-{int(time.time())}"))
        
        # Symbol
        message.setField(fix.Symbol(symbol))
        
        # SettlType - always 0 for SPOT as per documentation
        message.setField(fix.SettlType("0"))
        
        # Side
        side_val = fix.Side_BUY if side.upper() == "BUY" else fix.Side_SELL
        message.setField(fix.Side(side_val))
        
        # OrdType - 2 for Limit
        message.setField(fix.OrdType(fix.OrdType_LIMIT))
        
        # TimeInForce - 0 for Day
        message.setField(fix.TimeInForce(fix.TimeInForce_DAY))
        
        # Dealt currency
        message.setField(fix.Currency("EUR"))
        
        # OrderQty
        message.setField(fix.OrderQty(order_qty))
        
        # Price
        message.setField(fix.Price(price))
        
        # TransactTime - required
        transact_time = fix.TransactTime()
        transact_time.setString(fix.UtcTimeStamp().getString())
        message.setField(transact_time)
        
        return fix.Session.sendToTarget(message, self.session_id)


def main():
    try:
        # Create application settings
        settings = fix.SessionSettings("bosonic_client.cfg")
        application = BosonicApplication()
        store_factory = fix.FileStoreFactory(settings)
        log_factory = fix.FileLogFactory(settings)
        
        # Create initiator
        initiator = fix.SocketInitiator(application, store_factory, settings, log_factory)
        initiator.start()
        
        logger.info("FIX client started, waiting for logon...")
        
        # Wait for logon
        wait_count = 0
        while not application.logged_on and wait_count < 10:
            time.sleep(1)
            wait_count += 1
        
        if not application.logged_on:
            logger.error("Failed to log on within timeout")
            initiator.stop()
            return
            
        logger.info("Logged on successfully")
        
        # Request market data
        logger.info("Sending market data request...")
        application.send_market_data_request()
        
        # Wait for some time to receive market data
        time.sleep(10)
        
        # Send a test order if desired
        # logger.info("Sending test order...")
        # application.send_new_order_single()
        
        # Run for a while to receive messages
        time.sleep(30)
        
        logger.info("Shutting down...")
        initiator.stop()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
