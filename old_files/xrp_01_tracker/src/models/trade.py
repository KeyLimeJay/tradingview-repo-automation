#trade.py
class Trade:
    """
    Represents a trade from the Bitstamp API.
    """
    def __init__(self, trade_data, pair):
        """
        Initialize a Trade object from raw trade data.
        
        Args:
            trade_data: Raw trade data from Bitstamp API
            pair: Trading pair symbol (e.g., 'xrpusd')
        """
        self.id = trade_data['id']
        self.timestamp = int(trade_data['timestamp'])
        self.price = float(trade_data['price'])
        self.amount = float(trade_data['amount'])
        self.type = int(trade_data['type'])  # 0 = buy, 1 = sell
        self.pair = pair.upper()
        
    @property
    def side(self):
        """Return the trade side as a string (Buy/Sell)"""
        return 'Buy' if self.type == 0 else 'Sell'
    
    def to_dict(self):
        """Convert trade to dictionary for CSV storage"""
        return {
            'ID': self.id,
            'DateTime': self.format_datetime(),
            'Symbol': self.pair,
            'Quantity': self.amount,
            'Rate': self.price,
            'Side': self.side
        }
    
    def format_datetime(self):
        """Format timestamp as human-readable datetime string"""
        import datetime
        return datetime.datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S')