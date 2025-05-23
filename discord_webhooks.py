# discord_webhooks.py - Discord webhook integration

import requests
import json
import os
from datetime import datetime

def send_discord_webhook(webhook_url, content=None, embeds=None, max_retries=3):
    """
    Send a message to Discord via webhook
    
    Args:
        webhook_url (str): Discord webhook URL
        content (str, optional): Simple text content
        embeds (list, optional): List of embed objects
        max_retries (int): Number of retry attempts
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not webhook_url:
        print("No webhook URL provided, skipping Discord message")
        return False
    
    payload = {}
    if content:
        payload['content'] = content
    if embeds:
        payload['embeds'] = embeds
    
    headers = {'Content-Type': 'application/json'}
    
    for attempt in range(max_retries):
        try:
            response = requests.post(
                webhook_url, 
                data=json.dumps(payload), 
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 204:
                return True
            elif response.status_code == 429:  # Rate limited
                print(f"Discord rate limited, attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
            else:
                print(f"Discord webhook failed with status {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            print(f"Error sending Discord webhook (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                import time
                time.sleep(1)
    
    return False

def send_tradingview_data(gamma_results, webhook_url=None):
    """
    Send TradingView gamma flip data to Discord
    
    Args:
        gamma_results (list): List of gamma flip result strings
        webhook_url (str, optional): Discord webhook URL, will use env var if not provided
        
    Returns:
        bool: True if successful, False otherwise
    """
    if webhook_url is None:
        webhook_url = os.getenv('DISCORD_TRADINGVIEW_WEBHOOK')
    
    if not gamma_results:
        print("No gamma flip results to send")
        return False
    
    # Join all results with semicolons (TradingView format)
    content = ';'.join(gamma_results)
    
    # Discord has a 2000 character limit, so split if needed
    # Use 1800 as buffer for the wrapper text
    if len(content) > 1800:
        chunks = []
        current_chunk = ""
        
        for result in gamma_results:
            if len(current_chunk) + len(result) + 1 > 1800:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = result
            else:
                if current_chunk:
                    current_chunk += ";" + result
                else:
                    current_chunk = result
        
        if current_chunk:
            chunks.append(current_chunk)
        
        # Send each chunk as plain text (no formatting for easy copy-paste)
        success = True
        for i, chunk in enumerate(chunks):
            chunk_header = f"TradingView Data (Part {i+1}/{len(chunks)}):\n"
            chunk_content = chunk_header + chunk
            if not send_discord_webhook(webhook_url, content=chunk_content):
                success = False
        
        return success
    else:
        # Send as plain text for easy copy-paste to TradingView
        header = "TradingView Gamma Flip Data:\n"
        formatted_content = header + content
        return send_discord_webhook(webhook_url, content=formatted_content)

def send_overnight_sentiment(summary_df, webhook_url=None):
    """
    Send overnight sentiment analysis to Discord
    
    Args:
        summary_df (DataFrame): Overnight sentiment summary
        webhook_url (str, optional): Discord webhook URL, will use env var if not provided
        
    Returns:
        bool: True if successful, False otherwise
    """
    if webhook_url is None:
        webhook_url = os.getenv('DISCORD_OVERNIGHT_WEBHOOK')
    
    if summary_df is None or summary_df.empty:
        print("No overnight sentiment data to send")
        return False
    
    try:
        # Calculate overall market sentiment
        market_score = (summary_df['normalized_score'] * summary_df['openInterest_evening']).sum() / summary_df['openInterest_evening'].sum()
        market_direction = "🟢 BULLISH" if market_score > 0 else "🔴 BEARISH"
        
        # Get top movers
        bullish = summary_df[summary_df['sentiment'] == 'BULLISH'].head(5)
        bearish = summary_df[summary_df['sentiment'] == 'BEARISH'].tail(5).iloc[::-1]
        
        # Create embed
        embed = {
            "title": "🌙 Overnight Sentiment Analysis",
            "description": f"**Overall Market: {market_direction}** ({market_score:.2f})",
            "color": 0x00ff00 if market_score > 0 else 0xff0000,
            "timestamp": datetime.now().isoformat(),
            "fields": []
        }
        
        # Add bullish tickers
        if not bullish.empty:
            bullish_text = "\n".join([
                f"{row['ticker']}: {row['normalized_score']:.2f}" 
                for _, row in bullish.iterrows()
            ])
            embed["fields"].append({
                "name": "🟢 Top Bullish",
                "value": f"```\n{bullish_text}\n```",
                "inline": True
            })
        
        # Add bearish tickers
        if not bearish.empty:
            bearish_text = "\n".join([
                f"{row['ticker']}: {row['normalized_score']:.2f}" 
                for _, row in bearish.iterrows()
            ])
            embed["fields"].append({
                "name": "🔴 Top Bearish", 
                "value": f"```\n{bearish_text}\n```",
                "inline": True
            })
        
        return send_discord_webhook(webhook_url, embeds=[embed])
        
    except Exception as e:
        print(f"Error creating overnight sentiment message: {e}")
        return False

def send_daily_sentiment(summary_df, statistical_summary=None, webhook_url=None):
    """
    Send daily sentiment analysis to Discord
    
    Args:
        summary_df (DataFrame): Daily sentiment summary
        statistical_summary (DataFrame, optional): Statistical indicators summary
        webhook_url (str, optional): Discord webhook URL, will use env var if not provided
        
    Returns:
        bool: True if successful, False otherwise
    """
    if webhook_url is None:
        webhook_url = os.getenv('DISCORD_DAILY_WEBHOOK')
    
    # Handle case where we might only have statistical data
    has_options_data = summary_df is not None and not summary_df.empty
    has_statistical_data = statistical_summary is not None and not statistical_summary.empty
    
    if not has_options_data and not has_statistical_data:
        print("No daily sentiment data to send")
        return False
    
    try:
        embed = {
            "title": "📊 Daily Sentiment Analysis",
            "color": 0x0099ff,
            "timestamp": datetime.now().isoformat(),
            "fields": []
        }
        
        # Add options sentiment if available
        if has_options_data:
            # Calculate overall market sentiment
            market_score = (summary_df['normalized_score'] * summary_df['openInterest_previous']).sum() / summary_df['openInterest_previous'].sum()
            market_direction = "🟢 BULLISH" if market_score > 0 else "🔴 BEARISH"
            
            embed["description"] = f"**Overall Market: {market_direction}** ({market_score:.2f})"
            embed["color"] = 0x00ff00 if market_score > 0 else 0xff0000
            
            # Get top movers
            bullish = summary_df[summary_df['sentiment'] == 'BULLISH'].head(5)
            bearish = summary_df[summary_df['sentiment'] == 'BEARISH'].tail(5).iloc[::-1]
            
            # Add bullish tickers
            if not bullish.empty:
                bullish_text = "\n".join([
                    f"{row['ticker']}: {row['normalized_score']:.2f}" 
                    for _, row in bullish.iterrows()
                ])
                embed["fields"].append({
                    "name": "🟢 Top Bullish",
                    "value": f"```\n{bullish_text}\n```",
                    "inline": True
                })
            
            # Add bearish tickers
            if not bearish.empty:
                bearish_text = "\n".join([
                    f"{row['ticker']}: {row['normalized_score']:.2f}" 
                    for _, row in bearish.iterrows()
                ])
                embed["fields"].append({
                    "name": "🔴 Top Bearish",
                    "value": f"```\n{bearish_text}\n```",
                    "inline": True
                })
        
        # Add statistical indicators if available
        if has_statistical_data:
            stat_text = "\n".join([
                f"{row['ticker']}: {row['current_value']:.2f} ({row['pct_change']:.2f}%) - {row['sentiment']}"
                for _, row in statistical_summary.iterrows()
            ])
            embed["fields"].append({
                "name": "📈 Market Breadth",
                "value": f"```\n{stat_text}\n```",
                "inline": False
            })
        
        return send_discord_webhook(webhook_url, embeds=[embed])
        
    except Exception as e:
        print(f"Error creating daily sentiment message: {e}")
        return False