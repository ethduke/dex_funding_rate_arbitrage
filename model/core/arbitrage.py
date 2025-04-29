from typing import Dict, List

def find_arbitrage_opportunities(
    exchange_rates: Dict[str, Dict[str, Dict]], 
    min_diff: float = 0.00001
) -> List[Dict]:
    """
    Find arbitrage opportunities by comparing funding rates between exchanges.
    For each asset:
    - If funding rate is negative on exchange A, we go LONG on A (we receive funding)
    - If funding rate is positive on exchange B, we go SHORT on B (we pay funding)
    The profit comes from the difference in funding rates.
    
    Args:
        exchange_rates: Dict of exchange names to their asset rates
                       {exchange_name: {asset: {rate: float, ...}}}
        min_diff: Minimum funding rate difference to consider
    
    Returns:
        List of arbitrage opportunities with detailed exchange and action information
    """
    opportunities = []
    exchanges = list(exchange_rates.keys())
    
    for i in range(len(exchanges)):
        for j in range(i+1, len(exchanges)):
            ex1, ex2 = exchanges[i], exchanges[j]
            
            common_assets = set(exchange_rates[ex1].keys()) & set(exchange_rates[ex2].keys())
            
            for asset in common_assets:
                rate1 = exchange_rates[ex1][asset]["rate"]
                rate2 = exchange_rates[ex2][asset]["rate"]
                
                potential_profit = abs(rate1) + abs(rate2)
                
                if potential_profit >= min_diff:
                    long_ex = ex1 if rate1 < 0 else ex2
                    short_ex = ex2 if rate1 < 0 else ex1
                    long_rate = rate1 if rate1 < 0 else rate2
                    short_rate = rate2 if rate1 < 0 else rate1
                    
                    # Get additional exchange data
                    long_ex_data = exchange_rates[long_ex][asset]
                    short_ex_data = exchange_rates[short_ex][asset]
                    
                    opportunities.append({
                        "asset": asset,
                        "exchanges": [ex1, ex2],
                        "rates": {ex1: rate1, ex2: rate2},
                        "potential_profit": potential_profit,
                        "strategy": f"LONG on {long_ex} (rate: {long_rate:.6f}), SHORT on {short_ex} (rate: {short_rate:.6f})",
                        "estimated_daily_profit": potential_profit * 3,  # Assuming 8-hour funding periods
                        "actions": {
                            long_ex: {
                                "action": "LONG",
                                "rate": long_ex_data["rate"],
                                "next_funding_time": long_ex_data.get("next_funding_time", 0)
                            },
                            short_ex: {
                                "action": "SHORT",
                                "rate": short_ex_data["rate"],
                                "next_funding_time": short_ex_data.get("next_funding_time", 0)
                            }
                        },
                        "long_exchange": long_ex,
                        "short_exchange": short_ex
                    })
    
    # Sort by largest potential profit first
    opportunities.sort(key=lambda x: x["potential_profit"], reverse=True)
    return opportunities
