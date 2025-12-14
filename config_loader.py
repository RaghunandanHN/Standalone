import json
import os

def load_config():
    """Load configuration from config.json"""
    config_path = "config.json"
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    return config

def get_telegram_config(bot_name):
    """Get Telegram bot config by name"""
    config = load_config()
    return config['telegram_bots'].get(bot_name, {})

def get_settings():
    """Get general settings"""
    return load_config()