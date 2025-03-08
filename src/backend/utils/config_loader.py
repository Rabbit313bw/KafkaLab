import yaml
from pathlib import Path

def load_config():
    config_path = Path(__file__).parent.parent.parent.parent / 'config' / 'kafka_config.yaml'
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config['kafka']

def get_broker_list():
    config = load_config()
    return [f"{broker['host']}:{broker['port']}" for broker in config['brokers']]

def get_producer_config():
    return load_config()['producer_config']

def get_consumer_config():
    config = load_config()
    consumer_config = config['consumer_config'].copy()
    
    group_config = config['consumer_groups']['analysis_group']
    consumer_config.update({
        'group_id': group_config['group_id'],
        'auto_offset_reset': group_config['auto_offset_reset']
    })
    
    return consumer_config 