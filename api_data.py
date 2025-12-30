import requests
import pandas as pd
from datetime import datetime
import time
from typing import Optional, Dict, List

class FundingRateFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def fetch_apollox(self, symbol: Optional[str] = None, limit: int = 100) -> pd.DataFrame:
        """
        Fetch funding rate from ApolloX
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            limit: Number of records to return (max 1000)
            
        Returns:
            DataFrame with funding rate data
        """
        url = "https://fapi.apollox.finance/fapi/v1/fundingRate"
        params = {}
        
        if symbol:
            params['symbol'] = symbol
        if limit:
            params['limit'] = min(limit, 1000)  # API likely has a max limit
            
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data:
                df = pd.DataFrame(data)
                # Convert timestamp to readable datetime
                if 'fundingTime' in df.columns:
                    df['fundingTime_dt'] = pd.to_datetime(df['fundingTime'], unit='ms')
                    df['fundingRate'] = pd.to_numeric(df['fundingRate'], errors='coerce')
                return df
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error fetching ApolloX data: {e}")
            return pd.DataFrame()
    
    def fetch_binance(self, symbol: Optional[str] = None, limit: int = 100, 
                      start_time: Optional[int] = None, end_time: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch funding rate from Binance
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            limit: Number of records to return (max 1000)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            
        Returns:
            DataFrame with funding rate data
        """
        url = "https://fapi.binance.com/fapi/v1/fundingRate"
        params = {}
        
        if symbol:
            params['symbol'] = symbol
        if limit:
            params['limit'] = min(limit, 1000)
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
            
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data:
                df = pd.DataFrame(data)
                # Convert timestamp and numeric fields
                if 'fundingTime' in df.columns:
                    df['fundingTime_dt'] = pd.to_datetime(df['fundingTime'], unit='ms')
                    df['fundingRate'] = pd.to_numeric(df['fundingRate'], errors='coerce')
                    df['markPrice'] = pd.to_numeric(df['markPrice'], errors='coerce')
                return df
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error fetching Binance data: {e}")
            return pd.DataFrame()
    
    def fetch_bitmex(self, symbol: Optional[str] = None, count: int = 100, 
                     reverse: bool = True, start_time: Optional[str] = None) -> pd.DataFrame:
        """
        Fetch funding rate from Bitmex
        
        Args:
            symbol: Trading pair symbol (e.g., 'XBTUSD')
            count: Number of records to return
            reverse: True for newest first, False for oldest first
            start_time: Start time in ISO format (e.g., '2024-01-01T00:00:00.000Z')
            
        Returns:
            DataFrame with funding rate data
        """
        url = "https://www.bitmex.com/api/v1/funding"
        params = {
            'reverse': str(reverse).lower(),
            'count': min(count, 1000)  # Bitmex typically allows up to 1000
        }
        
        if symbol:
            params['symbol'] = symbol
        if start_time:
            params['startTime'] = start_time
            
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data:
                df = pd.DataFrame(data)
                # Convert timestamp
                if 'timestamp' in df.columns:
                    df['timestamp_dt'] = pd.to_datetime(df['timestamp'])
                    # Convert funding rates to float
                    numeric_cols = ['fundingRate', 'fundingRateDaily']
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                return df
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error fetching Bitmex data: {e}")
            return pd.DataFrame()
    
    def fetch_drift_s3(self, symbol: str, date: str) -> pd.DataFrame:
        """
        Attempt to fetch Drift funding rate from S3 bucket
        Note: This URL pattern may require specific authentication or headers
        
        Args:
            symbol: Market symbol
            date: Date in YYYYMMDD format (e.g., '20241229')
            
        Returns:
            DataFrame with funding rate data if successful
        """
        # Extract year, month, day from date
        if len(date) == 8:
            year = date[:4]
            month_day = date[4:]
        else:
            # Use current date as fallback
            today = datetime.now().strftime('%Y%m%d')
            year = today[:4]
            month_day = today[4:]
        
        # Construct the S3 URL pattern
        base_url = "https://drift-historical-data-v2.s3.eu-west-1.amazonaws.com"
        program_id = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"
        
        url = f"{base_url}/program/{program_id}/market/{symbol}/fundingRateRecords/{year}/{year}{month_day}"
        
        print(f"Attempting to fetch from: {url}")
        
        try:
            # Try with different headers that might work for S3
            headers = {
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = self.session.get(url, headers=headers, timeout=15)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                        return df
                    else:
                        print(f"Unexpected response format from Drift S3")
                        # Try to parse as CSV or other format
                        print(f"Response preview: {response.text[:200]}")
                except ValueError:
                    # Response might not be JSON
                    print(f"Response is not JSON. Content type: {response.headers.get('Content-Type')}")
                    print(f"First 500 chars: {response.text[:500]}")
            else:
                print(f"Failed to fetch Drift data. Status: {response.status_code}")
                print(f"Response: {response.text[:200]}")
                
        except Exception as e:
            print(f"Error fetching Drift S3 data: {e}")
            
        return pd.DataFrame()

    # def fetch_multiple_exchanges(self, symbol_mapping: Dict[str, str], 
    #                              limit_per_exchange: int = 50) -> Dict[str, pd.DataFrame]:
    #     """
    #     Fetch funding rates from multiple exchanges for comparable symbols
        
    #     Args:
    #         symbol_mapping: Dict with exchange names as keys and symbols as values
    #                        Example: {'binance': 'BTCUSDT', 'bitmex': 'XBTUSD'}
    #         limit_per_exchange: Number of records per exchange
        
    #     Returns:
    #         Dictionary with exchange names as keys and DataFrames as values
    #     """
    #     results = {}
        
    #     for exchange, symbol in symbol_mapping.items():
    #         print(f"Fetching from {exchange} for {symbol}...")
            
    #         if exchange.lower() == 'apollox':
    #             df = self.fetch_apollox(symbol, limit_per_exchange)
    #             results['apollox'] = df
                
    #         elif exchange.lower() == 'binance':
    #             df = self.fetch_binance(symbol, limit_per_exchange)
    #             results['binance'] = df
                
    #         elif exchange.lower() == 'bitmex':
    #             df = self.fetch_bitmex(symbol, limit_per_exchange)
    #             results['bitmex'] = df
                
    #         elif exchange.lower() == 'drift':
    #             # For Drift, we need a date - using today
    #             today = datetime.now().strftime('%Y%m%d')
    #             df = self.fetch_drift_s3(symbol, today)
    #             results['drift'] = df
            
    #         time.sleep(0.5)  # Rate limiting
        
    #     return results

# ==================== USAGE EXAMPLES ====================

def example_basic_usage():
    """Basic usage examples for each exchange"""
    fetcher = FundingRateFetcher()
    
    print("=" * 60)
    print("EXAMPLE 1: Fetch latest funding rates from Binance")
    print("=" * 60)
    
    # Get latest 10 BTC funding rates from Binance
    binance_data = fetcher.fetch_binance(symbol='BTCUSDT', limit=10)
    if not binance_data.empty:
        print("\nBinance BTCUSDT Funding Rates:")
        print(binance_data[['symbol', 'fundingTime_dt', 'fundingRate', 'markPrice']].to_string())
    else:
        print("No Binance data retrieved")
    
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Fetch from ApolloX for comparison")
    print("=" * 60)
    
    # Get ApolloX data for same symbol
    apollox_data = fetcher.fetch_apollox(symbol='BTCUSDT', limit=10)
    if not apollox_data.empty:
        print("\nApolloX BTCUSDT Funding Rates:")
        print(apollox_data[['symbol', 'fundingTime_dt', 'fundingRate']].to_string())
    
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Fetch historical data from Bitmex")
    print("=" * 60)
    
    # Get Bitmex data with optional start time
    # start_iso = '2024-01-01T00:00:00.000Z'  # Optional: specify start time
    bitmex_data = fetcher.fetch_bitmex(symbol='XBTUSD', count=5, reverse=True)
    if not bitmex_data.empty:
        print("\nBitmex XBTUSD Funding Rates:")
        print(bitmex_data[['symbol', 'timestamp_dt', 'fundingRate', 'fundingRateDaily']].to_string())
    
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Fetch from all exchanges for BTC")
    print("=" * 60)
    
    # # Fetch from multiple exchanges
    # symbol_map = {
    #     'binance': 'BTCUSDT',
    #     'apollox': 'BTCUSDT',
    #     'bitmex': 'XBTUSD'
    # }
    
    # all_data = fetcher.fetch_multiple_exchanges(symbol_map, limit_per_exchange=5)
    
    # for exchange, df in all_data.items():
    #     if not df.empty:
    #         print(f"\n{exchange.upper()} Data Shape: {df.shape}")
    #         print(f"Columns: {list(df.columns)}")
    #         if len(df) > 0:
    #             print(f"Latest funding rate: {df.iloc[0]['fundingRate'] if 'fundingRate' in df.columns else 'N/A'}")
    #     else:
    #         print(f"\nNo data retrieved from {exchange}")

def example_advanced_usage():
    """Advanced usage with time ranges and data analysis"""
    fetcher = FundingRateFetcher()
    
    print("=" * 60)
    print("ADVANCED: Fetch Binance data with time range")
    print("=" * 60)
    
    # Calculate timestamps for last 7 days
    end_time = int(datetime(2024, 2, 29).timestamp() * 1000)
    # end_time = int(time.time() * 1000)  # Current time in milliseconds
    # start_time = end_time - (7 * 24 * 60 * 60 * 1000)  # 7 days ago
    start_time = int(datetime(2023, 8, 1).timestamp() * 1000)
    
    btc_data = fetcher.fetch_binance(
        symbol='BTCUSDT',
        limit=5000,
        start_time=start_time,
        end_time=end_time
    )
    
    if not btc_data.empty:
        print(f"\nRetrieved {len(btc_data)} BTC funding rate records")
        
        # Basic analysis
        btc_data['fundingRate_pct'] = btc_data['fundingRate'] * 100  # Convert to percentage
        
        print(f"\nBTC Funding Rate Statistics (last 7 days):")
        print(f"Average: {btc_data['fundingRate_pct'].mean():.4f}%")
        print(f"Maximum: {btc_data['fundingRate_pct'].max():.4f}%")
        print(f"Minimum: {btc_data['fundingRate_pct'].min():.4f}%")
        print(f"Std Dev: {btc_data['fundingRate_pct'].std():.4f}%")
        
        # Count positive vs negative rates
        positive = (btc_data['fundingRate'] > 0).sum()
        negative = (btc_data['fundingRate'] < 0).sum()
        print(f"\nPositive rates: {positive}, Negative rates: {negative}")
        
        # Save to CSV
        btc_data.to_csv('binance_btc_funding_rates_2023_2024.csv', index=False)
        print("\nData saved to 'binance_btc_funding_rates_2023_2024.csv'")

    print("=" * 60)
    print("ADVANCED: Fetch Bitmex data with time range")
    print("=" * 60)

    # Get Bitmex data with optional start time
    
    btc_data = fetcher.fetch_bitmex(
        symbol='XBTUSD', 
        count=500,
        start_time = '2023-08-01T00:00:00.000Z',  # Optional: specify start time 
        reverse=True)
    
    if not btc_data.empty:
        print(f"\nRetrieved {len(btc_data)} BTC funding rate records")
        
        # Basic analysis
        btc_data['fundingRate_pct'] = btc_data['fundingRate'] * 100  # Convert to percentage
        
        print(f"\nBTC Funding Rate Statistics (last 7 days):")
        print(f"Average: {btc_data['fundingRate_pct'].mean():.4f}%")
        print(f"Maximum: {btc_data['fundingRate_pct'].max():.4f}%")
        print(f"Minimum: {btc_data['fundingRate_pct'].min():.4f}%")
        print(f"Std Dev: {btc_data['fundingRate_pct'].std():.4f}%")
        
        # Count positive vs negative rates
        positive = (btc_data['fundingRate'] > 0).sum()
        negative = (btc_data['fundingRate'] < 0).sum()
        print(f"\nPositive rates: {positive}, Negative rates: {negative}")
        
        # Save to CSV
        btc_data.to_csv('bitmex_btc_funding_rates_2023_2024.csv', index=False)
        print("\nData saved to 'bitmex_btc_funding_rates_2023_2024.csv'")

def test_drift_access():
    """Test function to try accessing Drift data"""
    fetcher = FundingRateFetcher()
    
    print("=" * 60)
    print("TESTING: Attempt to access Drift S3 data")
    print("=" * 60)
    
    # Note: You need to know the correct symbol format for Drift
    # This is just a test - the actual symbol format may differ
    test_symbol = "SOL-PERP"  # Example - you need to verify the correct symbol
    
    # Try with today's date
    today = datetime.now().strftime('%Y%m%d')
    
    drift_data = fetcher.fetch_drift_s3(test_symbol, today)
    
    if not drift_data.empty:
        print(f"\nSuccessfully retrieved Drift data!")
        print(f"Shape: {drift_data.shape}")
        print(f"Columns: {list(drift_data.columns)}")
        print(f"\nFirst few rows:")
        print(drift_data.head().to_string())
    else:
        print("\nCould not retrieve Drift data via S3 URL.")
        print("\nRecommendations for Drift data:")
        print("1. Check Drift's official documentation for API endpoints")
        print("2. Look for RPC endpoints or GraphQL APIs")
        print("3. Check if authentication is required")
        print("4. Verify the correct symbol format for their markets")

# ==================== MAIN EXECUTION ====================

if __name__ == "__main__":
    print("Funding Rate Data Fetcher")
    print("=" * 60)
    
    # Uncomment the examples you want to run:
    
    # Example 1: Basic usage
    # example_basic_usage()
    
    # Example 2: Advanced usage with time range
    example_advanced_usage()
    
    # Example 3: Test Drift access (likely won't work without correct setup)
    # test_drift_access()