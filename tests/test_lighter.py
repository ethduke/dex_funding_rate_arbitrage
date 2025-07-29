import asyncio
import logging
import time
import sys
import os
import json

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lighter
from utils.config import CONFIG
from model.exchanges.lighter import LighterExchange

logging.basicConfig(level=logging.INFO)

# Global storage for market stats
market_stats_data = {}

def on_order_book_update(market_id, order_book):
    logging.info(f"Order book {market_id}:\n{json.dumps(order_book, indent=2)}")

def on_account_update(account_id, account):
    logging.info(f"Account {account_id}:\n{json.dumps(account, indent=2)}")

def on_market_stats_update(market_stats):
    """Handle market stats updates from WebSocket"""
    try:
        market_id = market_stats.get("market_id")
        symbol = get_symbol_by_market_id(market_id)
        funding_rate = float(market_stats.get("current_funding_rate", 0))
        mark_price = float(market_stats.get("mark_price", 0))
        index_price = float(market_stats.get("index_price", 0))
        funding_timestamp = market_stats.get("funding_timestamp", 0)
        
        logging.info(f"📊 Market Stats [{symbol}]: Funding Rate={funding_rate}, Mark Price={mark_price}, Index Price={index_price}")
        
        # Store the latest market stats
        market_stats_data[market_id] = {
            "symbol": symbol,
            "funding_rate": funding_rate,
            "mark_price": mark_price,
            "index_price": index_price,
            "funding_timestamp": funding_timestamp,
            "market_stats": market_stats
        }
        
    except Exception as e:
        logging.error(f"Error processing market stats: {e}")

def get_symbol_by_market_id(market_id: int) -> str:
    """Get symbol name by market ID"""
    symbol_mapping = {
        0: "ETH",
        1: "BTC", 
        2: "SOL",
        8: "LINK",
        15: "TRUMP",
        21: "FARTCOIN",
        24: "HYPE",
        33: "KAITO",
        34: "IP",
    }
    return symbol_mapping.get(market_id, f"UNKNOWN_{market_id}")

def get_funding_rates() -> dict:
    """Get funding rates in normalized format"""
    result = {}
    for market_id, stats in market_stats_data.items():
        symbol = stats["symbol"]
        result[symbol] = {
            "rate": stats["funding_rate"],
            "mark_price": stats["mark_price"],
            "index_price": stats["index_price"],
            "next_funding_time": stats["funding_timestamp"],
            "exchange": "Lighter"
        }
    return result

async def test_lighter_funding_rates():
    """Test Lighter funding rates API"""
    
    print("Testing Lighter funding rates API...")
    
    try:
        # Create API client
        BASE_URL = CONFIG.LIGHTER_API_URL
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
        
        # Create funding API instance
        funding_api = lighter.FundingApi(api_client)
        
        print(f"API URL: {BASE_URL}")
        print("✅ Funding API client created successfully!")
        
        # Get funding rates
        print("Getting funding rates...")
        funding_rates = await funding_api.funding_rates()
        
        print(f"📈 Funding Rates Response:")
        print(json.dumps(funding_rates, indent=2, default=str))
        
        # Process funding rates into normalized format
        normalized_rates = {}
        if hasattr(funding_rates, 'funding_rates') and funding_rates.funding_rates:
            for rate in funding_rates.funding_rates:
                try:
                    market_id = rate.market_id
                    symbol = get_symbol_by_market_id(market_id)
                    normalized_rates[symbol] = {
                        "rate": float(rate.rate) if rate.rate else 0,
                        "exchange": rate.exchange,
                        "symbol": rate.symbol,
                        "market_id": market_id
                    }
                except Exception as e:
                    print(f"Error processing funding rate for market {rate.market_id}: {e}")
        
        print(f"\n📊 Normalized Funding Rates ({len(normalized_rates)} markets):")
        for symbol, data in normalized_rates.items():
            print(f"  {symbol}: {data['rate']} (Exchange: {data['exchange']})")
        
        await api_client.close()
        print("✅ Funding rates test completed!")
        
        return normalized_rates
        
    except Exception as e:
        print(f"❌ Error testing funding rates: {e}")
        import traceback
        traceback.print_exc()
        return {}

async def test_lighter_websocket_market_stats():
    """Test Lighter WebSocket market stats subscription"""
    
    print("Testing Lighter WebSocket market stats subscription...")
    
    try:
        # Create WebSocket client using our custom implementation
        from model.exchanges.lighter_ws import LighterWebSocketClient
        
        client = LighterWebSocketClient(
            order_book_ids=[0, 1],  # ETH and BTC markets
            account_ids=[1, 2],     # Account IDs
            on_order_book_update=on_order_book_update,
            on_account_update=on_account_update,
            on_market_stats_update=on_market_stats_update
        )
        
        # Start the WebSocket client
        print("Starting WebSocket client...")
        ws_task = asyncio.create_task(client.run_async())
        
        # Wait a bit for connection
        await asyncio.sleep(3)
        
        print("✅ WebSocket client started successfully!")
        print("📊 Receiving order book, account, and market stats updates...")
        
        # Wait for some updates to see the data flow
        print("Waiting for market data updates...")
        await asyncio.sleep(10)
        
        # Get current funding rates from WebSocket client
        funding_rates = client.get_funding_rates()
        print(f"\n📈 Current Funding Rates from WebSocket ({len(funding_rates)} markets):")
        for symbol, data in funding_rates.items():
            print(f"  {symbol}: {data['rate']} (Mark: {data['mark_price']}, Index: {data['index_price']})")
        
        # Get all market stats
        market_stats = client.get_market_stats()
        print(f"\n📊 All Market Stats from WebSocket ({len(market_stats)} markets):")
        for market_id, stats in market_stats.items():
            print(f"  Market {market_id} ({stats['symbol']}): Funding={stats['funding_rate']}, Mark={stats['mark_price']}")
        
        # Stop WebSocket client
        print("Stopping WebSocket client...")
        await client.close()
        ws_task.cancel()
        
        try:
            await ws_task
        except asyncio.CancelledError:
            pass
        
        print("✅ WebSocket market stats test completed!")
        
    except Exception as e:
        print(f"❌ Error testing WebSocket market stats: {e}")
        import traceback
        traceback.print_exc()

async def print_api(method, *args, **kwargs):
    """Helper function to print API results"""
    try:
        result = await method(*args, **kwargs)
        logging.info(f"{method.__name__}: {result}")
        return result
    except Exception as e:
        logging.error(f"Error in {method.__name__}: {e}")
        raise

async def account_apis(client: lighter.ApiClient):
    """Test account APIs functionality"""
    logging.info("ACCOUNT APIS")
    account_instance = lighter.AccountApi(client)
    
    # Example values - you'll need to replace these with real values
    L1_ADDRESS = "0x9ed80eDA7F55054Db9FB5282451688f26bB374c1"  # Example address
    ACCOUNT_INDEX = 1  # Example account index
    
    try:
        await print_api(account_instance.account, by="l1_address", value=L1_ADDRESS)
        await print_api(account_instance.account, by="index", value=str(ACCOUNT_INDEX))
        await print_api(account_instance.accounts_by_l1_address, l1_address=L1_ADDRESS)
        await print_api(account_instance.apikeys, account_index=ACCOUNT_INDEX, api_key_index=1)
        await print_api(account_instance.public_pools, filter="all", limit=1, index=0)
        
        print("✅ Account APIs test completed successfully!")
        
    except Exception as e:
        print(f"❌ Error testing account APIs: {e}")
        import traceback
        traceback.print_exc()

async def test_lighter_api_structure():
    """Test Lighter API structure and basic functionality"""
    
    print("Testing Lighter API structure...")
    
    try:
        # Create API client
        BASE_URL = CONFIG.LIGHTER_API_URL
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
        
        print(f"API URL: {BASE_URL}")
        print("✅ API client created successfully!")
        
        # Test account APIs
        await account_apis(api_client)
        
        # Test available API methods
        print("\n=== Available API Methods ===")
        account_api = lighter.AccountApi(api_client)
        account_methods = [method for method in dir(account_api) if not method.startswith('_')]
        for method in account_methods:
            print(f"  - {method}")
        
        await api_client.close()
        print("✅ API structure test completed!")
        
    except Exception as e:
        print(f"❌ Error testing API structure: {e}")
        import traceback
        traceback.print_exc()

async def test_lighter_functionality():
    """Test basic Lighter functionality"""
    
    await test_lighter_api_structure()
    
    # Test funding rates API
    await test_lighter_funding_rates()
    
    # Test WebSocket market stats
    await test_lighter_websocket_market_stats()

async def compare_funding_rates():
    """Compare funding rates between Lighter and Hyperliquid"""
    
    print("Comparing funding rates between Lighter and Hyperliquid...")
    
    try:
        # Get funding rates from Lighter
        BASE_URL = CONFIG.LIGHTER_API_URL
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
        funding_api = lighter.FundingApi(api_client)
        
        funding_rates = await funding_api.funding_rates()
        
        # Process funding rates by exchange
        rates_by_exchange = {}
        if hasattr(funding_rates, 'funding_rates') and funding_rates.funding_rates:
            for rate in funding_rates.funding_rates:
                exchange = rate.exchange
                symbol = rate.symbol
                funding_rate = float(rate.rate) if rate.rate else 0
                
                if exchange not in rates_by_exchange:
                    rates_by_exchange[exchange] = {}
                
                rates_by_exchange[exchange][symbol] = {
                    "rate": funding_rate,
                    "market_id": rate.market_id
                }
        
        # Compare Lighter vs Hyperliquid
        lighter_rates = rates_by_exchange.get('lighter', {})
        hyperliquid_rates = rates_by_exchange.get('hyperliquid', {})
        
        print(f"\n📊 Funding Rate Comparison:")
        print(f"Lighter markets: {len(lighter_rates)}")
        print(f"Hyperliquid markets: {len(hyperliquid_rates)}")
        
        # Find common symbols
        common_symbols = set(lighter_rates.keys()) & set(hyperliquid_rates.keys())
        print(f"Common symbols: {len(common_symbols)}")
        
        if common_symbols:
            print(f"\n🔍 Detailed Comparison (Common Symbols):")
            print(f"{'Symbol':<15} {'Lighter':<12} {'Hyperliquid':<12} {'Difference':<12} {'Opportunity':<15}")
            print("-" * 70)
            
            for symbol in sorted(common_symbols):
                lighter_rate = lighter_rates[symbol]['rate']
                hyperliquid_rate = hyperliquid_rates[symbol]['rate']
                difference = lighter_rate - hyperliquid_rate
                
                # Determine arbitrage opportunity
                if abs(difference) > 0.0001:  # 0.01% threshold
                    if difference > 0:
                        opportunity = "Lighter > Hyperliquid"
                    else:
                        opportunity = "Hyperliquid > Lighter"
                else:
                    opportunity = "No significant diff"
                
                print(f"{symbol:<15} {lighter_rate:<12.6f} {hyperliquid_rate:<12.6f} {difference:<12.6f} {opportunity:<15}")
        
        # Show top opportunities
        if common_symbols:
            opportunities = []
            for symbol in common_symbols:
                lighter_rate = lighter_rates[symbol]['rate']
                hyperliquid_rate = hyperliquid_rates[symbol]['rate']
                difference = abs(lighter_rate - hyperliquid_rate)
                opportunities.append((symbol, difference, lighter_rate, hyperliquid_rate))
            
            opportunities.sort(key=lambda x: x[1], reverse=True)
            
            print(f"\n🎯 Top Arbitrage Opportunities:")
            print(f"{'Symbol':<15} {'Difference':<12} {'Lighter':<12} {'Hyperliquid':<12}")
            print("-" * 55)
            
            for symbol, diff, l_rate, h_rate in opportunities[:10]:
                print(f"{symbol:<15} {diff:<12.6f} {l_rate:<12.6f} {h_rate:<12.6f}")
        
        await api_client.close()
        print("✅ Funding rate comparison completed!")
        
    except Exception as e:
        print(f"❌ Error comparing funding rates: {e}")
        import traceback
        traceback.print_exc()

async def discover_markets():
    """Discover all available markets from Lighter API"""
    
    print("Discovering all available markets from Lighter API...")
    
    try:
        # Create API client
        BASE_URL = CONFIG.LIGHTER_API_URL
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
        
        # Create funding API instance
        funding_api = lighter.FundingApi(api_client)
        
        print(f"API URL: {BASE_URL}")
        print("✅ Funding API client created successfully!")
        
        # Get funding rates to discover all markets
        print("Getting funding rates to discover markets...")
        funding_rates = await funding_api.funding_rates()
        
        # Extract unique market information
        markets = {}
        if hasattr(funding_rates, 'funding_rates') and funding_rates.funding_rates:
            for rate in funding_rates.funding_rates:
                market_id = rate.market_id
                symbol = rate.symbol
                exchange = rate.exchange
                
                if market_id not in markets:
                    markets[market_id] = {
                        "market_id": market_id,
                        "symbol": symbol,
                        "exchange": exchange,
                        "funding_rate": float(rate.rate) if rate.rate else 0
                    }
        
        print(f"\n📊 Discovered Markets ({len(markets)} markets):")
        print(f"{'Market ID':<10} {'Symbol':<15} {'Exchange':<15} {'Funding Rate':<15}")
        print("-" * 60)
        
        for market_id, market_info in sorted(markets.items()):
            print(f"{market_id:<10} {market_info['symbol']:<15} {market_info['exchange']:<15} {market_info['funding_rate']:<15.6f}")
        
        # Create symbol mapping for easy reference
        symbol_mapping = {market_id: info['symbol'] for market_id, info in markets.items()}
        print(f"\n🔧 Symbol Mapping for Code:")
        print("symbol_mapping = {")
        for market_id, symbol in sorted(symbol_mapping.items()):
            print(f"    {market_id}: \"{symbol}\",")
        print("}")
        
        await api_client.close()
        print("✅ Market discovery completed!")
        
        return markets
        
    except Exception as e:
        print(f"❌ Error discovering markets: {e}")
        import traceback
        traceback.print_exc()
        return {}

async def test_market_mapping():
    """Test that market mapping works correctly"""
    
    print("Testing market mapping...")
    
    try:
        # Create Lighter exchange instance
        lt_client = LighterExchange(use_ws=False)
        
        # Test some known market IDs
        test_cases = [
            (0, "ETH"),
            (1, "BTC"),
            (2, "SOL"),
            (8, "LINK"),
            (15, "TRUMP"),
            (21, "FARTCOIN"),
            (24, "HYPE"),
            (33, "KAITO"),
            (34, "IP"),
            (56, "ZK"),
        ]
        
        print("Testing market ID to symbol mapping:")
        for market_id, expected_symbol in test_cases:
            actual_symbol = await lt_client._get_symbol_by_market_id(market_id)
            status = "✅" if actual_symbol == expected_symbol else "❌"
            print(f"  {status} Market {market_id} -> {actual_symbol} (expected: {expected_symbol})")
        
        # Test reverse mapping
        print("\nTesting symbol to market ID mapping:")
        for market_id, symbol in test_cases:
            actual_market_id = await lt_client._get_market_id(symbol)
            status = "✅" if actual_market_id == market_id else "❌"
            print(f"  {status} {symbol} -> Market {actual_market_id} (expected: {market_id})")
        
        print("✅ Market mapping test completed!")
        
    except Exception as e:
        print(f"❌ Error testing market mapping: {e}")
        import traceback
        traceback.print_exc()

async def test_market_utilities():
    """Test the new market utility methods"""
    
    print("Testing market utility methods...")
    
    try:
        # Create Lighter exchange instance
        lt_client = LighterExchange(use_ws=False)
        
        # Test market count
        market_count = lt_client.get_market_count()
        print(f"📊 Total markets: {market_count}")
        
        # Test listing all markets
        all_markets = lt_client.list_all_markets()
        print(f"📋 All markets ({len(all_markets)}):")
        for market_id, symbol in all_markets:
            print(f"  Market {market_id}: {symbol}")
        
        # Test getting all markets from API (if available)
        try:
            api_markets = await lt_client.get_all_markets()
            print(f"🌐 Markets from API: {len(api_markets)}")
            for market_id, symbol in sorted(api_markets.items()):
                print(f"  Market {market_id}: {symbol}")
        except Exception as e:
            print(f"⚠️  Could not fetch from API: {e}")
        
        print("✅ Market utilities test completed!")
        
    except Exception as e:
        print(f"❌ Error testing market utilities: {e}")
        import traceback
        traceback.print_exc()

async def test_dynamic_market_mapping():
    """Test the new dynamic market mapping approach"""
    
    print("Testing dynamic market mapping...")
    
    try:
        # Create Lighter exchange instance
        lt_client = LighterExchange(use_ws=False)
        
        # Initially, cache should be empty
        initial_count = lt_client.get_market_count()
        print(f"📊 Initial market count: {initial_count}")
        
        # Fetch markets from API
        markets = await lt_client.get_all_markets()
        print(f"🌐 Fetched {len(markets)} markets from API")
        
        # Now cache should be populated
        updated_count = lt_client.get_market_count()
        print(f"📊 Updated market count: {updated_count}")
        
        # Test symbol mapping
        test_symbol = await lt_client._get_symbol_by_market_id(0)
        print(f"✅ Market 0 -> {test_symbol}")
        
        # Test reverse mapping
        test_market_id = await lt_client._get_market_id("ETH")
        print(f"✅ ETH -> Market {test_market_id}")
        
        # List all markets
        all_markets = lt_client.list_all_markets()
        print(f"📋 Total markets listed: {len(all_markets)}")
        
        print("✅ Dynamic market mapping test completed!")
        
    except Exception as e:
        print(f"❌ Error testing dynamic market mapping: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Run the market discovery
    asyncio.run(discover_markets())
    # Run the market mapping test
    asyncio.run(test_market_mapping())
    # Run the market utilities test
    asyncio.run(test_market_utilities())
    # Run the dynamic market mapping test
    asyncio.run(test_dynamic_market_mapping()) 