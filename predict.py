#!/usr/bin/env python3
"""
Use the trained model to predict fraud on new data.
Can be used for testing individual records or batch prediction.
"""
import pickle
import pandas as pd
import argparse
import json


def load_model(model_path='fraud_model.pkl'):
    """Load the trained model and encoders."""
    with open(model_path, 'rb') as f:
        model_data = pickle.load(f)
    return model_data


def predict_fraud(data, model_data):
    """
    Predict fraud for given data.
    
    Args:
        data: dict or DataFrame with features
        model_data: loaded model data from pickle
    
    Returns:
        prediction (0 or 1) and probability
    """
    model = model_data['model']
    label_encoders = model_data['label_encoders']
    numeric_features = model_data['numeric_features']
    categorical_features = model_data['categorical_features']
    
    # Convert to DataFrame if dict
    if isinstance(data, dict):
        df = pd.DataFrame([data])
    else:
        df = data.copy()
    
    # Prepare features
    X = pd.DataFrame()
    
    # Numeric features
    for col in numeric_features:
        X[col] = df[col] if col in df.columns else 0
    
    # Categorical features (encode)
    for col in categorical_features:
        if col in df.columns:
            le = label_encoders[col]
            # Handle unseen categories
            try:
                X[col] = le.transform(df[col].astype(str))
            except ValueError:
                # If category not seen during training, use most common class
                X[col] = 0
        else:
            X[col] = 0
    
    # Predict
    prediction = model.predict(X)[0]
    probability = model.predict_proba(X)[0]
    
    return {
        'is_fraudulent': int(prediction),
        'fraud_probability': float(probability[1]),
        'confidence': float(max(probability))
    }


def predict_from_json(json_str, model_path='fraud_model.pkl'):
    """Predict fraud from JSON string (for Kafka integration)."""
    data = json.loads(json_str) if isinstance(json_str, str) else json_str
    model_data = load_model(model_path)
    return predict_fraud(data, model_data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Predict fraud using trained model')
    parser.add_argument('--model', '-m', default='fraud_model.pkl', 
                        help='Path to trained model pickle file')
    parser.add_argument('--interactive', '-i', action='store_true',
                        help='Interactive mode: enter parameters manually')
    parser.add_argument('--json', '-j', type=str,
                        help='JSON string with features')
    parser.add_argument('--test', '-t', action='store_true',
                        help='Run with test data')
    args = parser.parse_args()
    
    model_data = load_model(args.model)
    print(f"✓ Model loaded from {args.model}")
    
    if args.test:
        # Test with sample data
        test_cases = [
            {
                "click_duration": 0.29,
                "scroll_depth": 60,
                "mouse_movement": 111,
                "keystrokes_detected": 8,
                "click_frequency": 7,
                "time_since_last_click": 72,
                "VPN_usage": 0,
                "proxy_usage": 1,
                "bot_likelihood_score": 0.29,
                "device_type": "Tablet",
                "browser": "Safari",
                "operating_system": "Android",
                "device_ip_reputation": "Good",
                "ad_position": "Bottom"
            },
            {
                "click_duration": 7.91,
                "scroll_depth": 24,
                "mouse_movement": 145,
                "keystrokes_detected": 34,
                "click_frequency": 1,
                "time_since_last_click": 517,
                "VPN_usage": 0,
                "proxy_usage": 0,
                "bot_likelihood_score": 0.94,
                "device_type": "Desktop",
                "browser": "Edge",
                "operating_system": "iOS",
                "device_ip_reputation": "Good",
                "ad_position": "Bottom"
            }
        ]
        
        for i, case in enumerate(test_cases, 1):
            print(f"\n{'='*50}")
            print(f"Test Case {i}:")
            print(f"  Bot Score: {case['bot_likelihood_score']}")
            print(f"  Click Duration: {case['click_duration']}s")
            print(f"  Device: {case['device_type']}, {case['browser']}")
            result = predict_fraud(case, model_data)
            print(f"\n  Prediction: {'FRAUD' if result['is_fraudulent'] else 'LEGITIMATE'}")
            print(f"  Fraud Probability: {result['fraud_probability']:.2%}")
            print(f"  Confidence: {result['confidence']:.2%}")
    
    elif args.json:
        result = predict_from_json(args.json, args.model)
        print(json.dumps(result, indent=2))
    
    elif args.interactive:
        print("\n" + "="*50)
        print("INTERACTIVE FRAUD PREDICTION")
        print("="*50)
        print("Enter values for each parameter (press Enter for default):\n")
        
        params = {}
        params['click_duration'] = float(input("Click duration (seconds) [1.0]: ") or 1.0)
        params['scroll_depth'] = int(input("Scroll depth (%) [50]: ") or 50)
        params['mouse_movement'] = int(input("Mouse movement (pixels) [200]: ") or 200)
        params['keystrokes_detected'] = int(input("Keystrokes detected [10]: ") or 10)
        params['click_frequency'] = int(input("Click frequency [5]: ") or 5)
        params['time_since_last_click'] = int(input("Time since last click (seconds) [100]: ") or 100)
        params['VPN_usage'] = int(input("VPN usage (0 or 1) [0]: ") or 0)
        params['proxy_usage'] = int(input("Proxy usage (0 or 1) [0]: ") or 0)
        params['bot_likelihood_score'] = float(input("Bot likelihood score (0-1) [0.5]: ") or 0.5)
        params['device_type'] = input("Device type (Desktop/Mobile/Tablet) [Desktop]: ") or "Desktop"
        params['browser'] = input("Browser (Chrome/Firefox/Safari/Edge/Opera) [Chrome]: ") or "Chrome"
        params['operating_system'] = input("OS (Windows/iOS/Android) [Windows]: ") or "Windows"
        params['device_ip_reputation'] = input("IP Reputation (Good/Suspicious/Bad) [Good]: ") or "Good"
        params['ad_position'] = input("Ad Position (Top/Bottom/Side) [Top]: ") or "Top"
        
        result = predict_fraud(params, model_data)
        print("\n" + "="*50)
        print("PREDICTION RESULT")
        print("="*50)
        print(f"Is Fraudulent: {'YES' if result['is_fraudulent'] else 'NO'}")
        print(f"Fraud Probability: {result['fraud_probability']:.2%}")
        print(f"Confidence: {result['confidence']:.2%}")
    
    else:
        print("\nUsage:")
        print("  --test          Run with test cases")
        print("  --interactive   Enter parameters manually")
        print("  --json '{...}'  Predict from JSON string")
