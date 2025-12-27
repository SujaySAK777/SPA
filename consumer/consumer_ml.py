#!/usr/bin/env python3
"""
ML-Enhanced Kafka consumer that uses trained model to predict fraud.
Compares ML predictions with actual labels from the stream.
"""
import argparse
import json
import time
import pickle
from kafka import KafkaConsumer


def load_model(model_path='fraud_model.pkl'):
    """Load the trained model and encoders."""
    with open(model_path, 'rb') as f:
        model_data = pickle.load(f)
    return model_data


def predict_fraud(data, model_data):
    """Predict fraud using the trained ML model."""
    import pandas as pd
    
    model = model_data['model']
    label_encoders = model_data['label_encoders']
    numeric_features = model_data['numeric_features']
    categorical_features = model_data['categorical_features']
    
    # Prepare features
    X = pd.DataFrame()
    
    # Numeric features
    for col in numeric_features:
        X[col] = [data.get(col, 0)]
    
    # Categorical features (encode)
    for col in categorical_features:
        if col in data:
            le = label_encoders[col]
            try:
                X[col] = le.transform([str(data[col])])
            except ValueError:
                X[col] = [0]
        else:
            X[col] = [0]
    
    # Predict
    prediction = model.predict(X)[0]
    probability = model.predict_proba(X)[0]
    
    return {
        'prediction': int(prediction),
        'fraud_probability': float(probability[1]),
        'confidence': float(max(probability))
    }


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument("--broker", "-b", default="localhost:9092", help="Kafka bootstrap server")
    p.add_argument("--topic", "-t", default="output-topic", help="Topic to consume from")
    p.add_argument("--group", default="ml-verify-group")
    p.add_argument("--max", type=int, default=0, help="Exit after reading this many messages (0 = infinite)")
    p.add_argument("--delay", "-d", type=float, default=1.5, help="Delay in seconds between messages")
    p.add_argument("--model", "-m", default="fraud_model.pkl", help="Path to trained model")
    args = p.parse_args()

    # Load ML model
    print(f"Loading ML model from {args.model}...")
    model_data = load_model(args.model)
    print("✓ Model loaded successfully\n")
    
    consumer = KafkaConsumer(args.topic,
                             bootstrap_servers=[args.broker],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id=args.group,
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    count = 0
    correct_predictions = 0
    
    print("="*70)
    print("ML FRAUD DETECTION - Real-time Prediction vs Actual Labels")
    print("="*70)
    
    try:
        for msg in consumer:
            count += 1
            data = msg.value
            
            # Get ML prediction
            ml_result = predict_fraud(data, model_data)
            actual_label = data.get('is_fraudulent', -1)
            
            # Check if prediction matches
            is_correct = ml_result['prediction'] == actual_label
            if is_correct:
                correct_predictions += 1
            
            # Display
            print(f"\n{'='*70}")
            print(f"Message #{count} - Click ID: {data.get('click_id', 'N/A')[:20]}...")
            print(f"{'='*70}")
            print(f"IP Address: {data.get('ip_address', 'N/A')}")
            print(f"Device: {data.get('device_type', 'N/A')} | Browser: {data.get('browser', 'N/A')}")
            print(f"Bot Score: {data.get('bot_likelihood_score', 0):.2f} | Click Duration: {data.get('click_duration', 0):.2f}s")
            print(f"\n{'─'*70}")
            print(f"ML PREDICTION: {'🚨 FRAUD' if ml_result['prediction'] == 1 else '✓ LEGITIMATE'}")
            print(f"Fraud Probability: {ml_result['fraud_probability']:.2%} | Confidence: {ml_result['confidence']:.2%}")
            print(f"{'─'*70}")
            print(f"ACTUAL LABEL:  {'🚨 FRAUD' if actual_label == 1 else '✓ LEGITIMATE'}")
            print(f"{'─'*70}")
            
            if is_correct:
                print(f"✅ CORRECT PREDICTION")
            else:
                print(f"❌ INCORRECT PREDICTION")
            
            print(f"\nAccuracy so far: {correct_predictions}/{count} ({correct_predictions/count*100:.1f}%)")
            
            if args.max and count >= args.max:
                break
            
            # Add delay between messages
            if args.delay > 0 and (not args.max or count < args.max):
                time.sleep(args.delay)
    
    except KeyboardInterrupt:
        print("\n\nStopping consumer...")
    finally:
        consumer.close()
        print(f"\n{'='*70}")
        print(f"FINAL RESULTS")
        print(f"{'='*70}")
        print(f"Total Messages: {count}")
        print(f"Correct Predictions: {correct_predictions}")
        print(f"Accuracy: {correct_predictions/count*100:.1f}%" if count > 0 else "N/A")
        print(f"{'='*70}")
