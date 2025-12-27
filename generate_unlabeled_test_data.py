#!/usr/bin/env python3
"""
Generate test data WITHOUT the 'is_fraudulent' column
This simulates real-world scenario where we don't know the ground truth
and need the ML model to predict fraud.
"""
import pandas as pd
import random
import uuid
from datetime import datetime, timedelta
import argparse


def generate_random_click():
    """Generate a random click event without fraud label"""
    
    device_types = ['Desktop', 'Mobile', 'Tablet']
    browsers = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera']
    operating_systems = ['Windows', 'macOS', 'Linux', 'iOS', 'Android']
    ad_positions = ['Top', 'Bottom', 'Sidebar', 'Middle']
    reputations = ['Good', 'Suspicious', 'Bad']
    
    # Generate random values
    click = {
        'click_id': str(uuid.uuid4()),
        'timestamp': (datetime.now() - timedelta(seconds=random.randint(0, 3600))).strftime('%Y-%m-%d %H:%M:%S'),
        'user_id': str(uuid.uuid4()),
        'ip_address': f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
        'device_type': random.choice(device_types),
        'browser': random.choice(browsers),
        'operating_system': random.choice(operating_systems),
        'referrer_url': f"https://example{random.randint(1, 100)}.com/",
        'page_url': f"https://landing{random.randint(1, 100)}.com/",
        'click_duration': round(random.uniform(0.01, 5.0), 2),
        'scroll_depth': random.randint(0, 100),
        'mouse_movement': random.randint(0, 200),
        'keystrokes_detected': random.randint(0, 20),
        'ad_position': random.choice(ad_positions),
        'click_frequency': random.randint(1, 50),
        'time_since_last_click': random.randint(1, 300),
        'device_ip_reputation': random.choice(reputations),
        'VPN_usage': random.choice([0, 1]),
        'proxy_usage': random.choice([0, 1]),
        'bot_likelihood_score': round(random.uniform(0.0, 1.0), 2),
    }
    # NOTE: NO 'is_fraudulent' column!
    
    return click


def generate_suspicious_click():
    """Generate a click with suspicious characteristics (likely fraud)"""
    
    click = {
        'click_id': str(uuid.uuid4()),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'user_id': str(uuid.uuid4()),
        'ip_address': f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
        'device_type': random.choice(['Desktop', 'Mobile']),
        'browser': random.choice(['Chrome', 'Firefox']),
        'operating_system': random.choice(['Windows', 'Linux']),
        'referrer_url': 'https://suspicious-site.com/',
        'page_url': 'https://ad-landing.com/',
        'click_duration': round(random.uniform(0.01, 0.1), 2),  # Very short
        'scroll_depth': random.randint(0, 10),  # Little scrolling
        'mouse_movement': random.randint(0, 20),  # Little movement
        'keystrokes_detected': 0,  # No keystrokes
        'ad_position': 'Bottom',
        'click_frequency': random.randint(20, 50),  # High frequency
        'time_since_last_click': random.randint(1, 5),  # Very quick
        'device_ip_reputation': random.choice(['Suspicious', 'Bad']),
        'VPN_usage': 1,
        'proxy_usage': 1,
        'bot_likelihood_score': round(random.uniform(0.7, 0.99), 2),  # High bot score
    }
    
    return click


def generate_legitimate_click():
    """Generate a click with legitimate characteristics"""
    
    click = {
        'click_id': str(uuid.uuid4()),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'user_id': str(uuid.uuid4()),
        'ip_address': f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
        'device_type': random.choice(['Desktop', 'Mobile', 'Tablet']),
        'browser': random.choice(['Chrome', 'Safari', 'Firefox']),
        'operating_system': random.choice(['Windows', 'macOS', 'iOS', 'Android']),
        'referrer_url': 'https://google.com/',
        'page_url': 'https://legitimate-site.com/',
        'click_duration': round(random.uniform(1.0, 5.0), 2),  # Reasonable duration
        'scroll_depth': random.randint(40, 100),  # Good engagement
        'mouse_movement': random.randint(80, 150),  # Natural movement
        'keystrokes_detected': random.randint(5, 15),  # Some typing
        'ad_position': random.choice(['Top', 'Middle']),
        'click_frequency': random.randint(1, 10),  # Low frequency
        'time_since_last_click': random.randint(30, 200),  # Reasonable timing
        'device_ip_reputation': 'Good',
        'VPN_usage': 0,
        'proxy_usage': 0,
        'bot_likelihood_score': round(random.uniform(0.0, 0.3), 2),  # Low bot score
    }
    
    return click


def main():
    parser = argparse.ArgumentParser(description='Generate unlabeled test data for fraud detection')
    parser.add_argument('--output', '-o', default='test_clicks_unlabeled.csv', help='Output CSV file')
    parser.add_argument('--count', '-n', type=int, default=100, help='Number of test records to generate')
    parser.add_argument('--suspicious-ratio', type=float, default=0.3, help='Ratio of suspicious clicks (0-1)')
    parser.add_argument('--legitimate-ratio', type=float, default=0.3, help='Ratio of legitimate clicks (0-1)')
    args = parser.parse_args()
    
    print(f"🔧 Generating {args.count} test click records...")
    print(f"   - {int(args.count * args.suspicious_ratio)} suspicious clicks (likely fraud)")
    print(f"   - {int(args.count * args.legitimate_ratio)} legitimate clicks")
    print(f"   - {args.count - int(args.count * args.suspicious_ratio) - int(args.count * args.legitimate_ratio)} random clicks")
    print(f"   - NO 'is_fraudulent' column (true test scenario!)")
    print()
    
    clicks = []
    
    # Generate suspicious clicks
    suspicious_count = int(args.count * args.suspicious_ratio)
    for _ in range(suspicious_count):
        clicks.append(generate_suspicious_click())
    
    # Generate legitimate clicks
    legitimate_count = int(args.count * args.legitimate_ratio)
    for _ in range(legitimate_count):
        clicks.append(generate_legitimate_click())
    
    # Generate random clicks
    random_count = args.count - suspicious_count - legitimate_count
    for _ in range(random_count):
        clicks.append(generate_random_click())
    
    # Shuffle
    random.shuffle(clicks)
    
    # Create DataFrame
    df = pd.DataFrame(clicks)
    
    # Verify no 'is_fraudulent' column
    assert 'is_fraudulent' not in df.columns, "ERROR: is_fraudulent column should not exist!"
    
    # Save to CSV
    df.to_csv(args.output, index=False)
    
    print(f"✅ Generated {len(df)} test records")
    print(f"📄 Saved to: {args.output}")
    print(f"\n📊 Columns in test data:")
    for col in df.columns:
        print(f"   - {col}")
    print(f"\n✓ Confirmed: 'is_fraudulent' column is NOT present")
    print(f"\n🚀 Ready to test! Use this file with the producer:")
    print(f"   python producer/producer.py --file {args.output} --broker localhost:9092 --rate 50")


if __name__ == '__main__':
    main()
