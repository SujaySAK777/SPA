#!/usr/bin/env python3
"""
ML-Enhanced Kafka consumer that shows both:
1. Flink's filtering result (messages in output-topic are already flagged as fraud)
2. ML model's prediction and confidence score
"""
import argparse
import json
import time
import pickle
from kafka import KafkaConsumer


class FraudPredictor:
    def __init__(self, model_path='fraud_model.pkl'):
        """Load the trained model and encoders."""
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model = model_data['model']
        self.label_encoders = model_data['label_encoders']
        self.numeric_features = model_data['numeric_features']
        self.categorical_features = model_data['categorical_features']
    
    def prepare_features(self, data):
        """Prepare features from JSON data for prediction."""
        features = []
        
        for feature in self.numeric_features:
            value = data.get(feature, 0)
            features.append(float(value) if value is not None else 0.0)
        
        for feature in self.categorical_features:
            value = str(data.get(feature, 'Unknown'))
            if feature in self.label_encoders:
                le = self.label_encoders[feature]
                try:
                    encoded_value = le.transform([value])[0]
                except ValueError:
                    encoded_value = 0
            else:
                encoded_value = 0
            features.append(encoded_value)
        
        return features
    
    def predict(self, data):
        """Predict if a click is fraudulent."""
        features = self.prepare_features(data)
        prediction = self.model.predict([features])[0]
        probability = self.model.predict_proba([features])[0]
        return int(prediction), float(probability[1])


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument("--broker", "-b", default="localhost:9092", help="Kafka bootstrap server")
    p.add_argument("--topic", "-t", default="output-topic", help="Topic to consume from")
    p.add_argument("--group", default="ml-verify-group")
    p.add_argument("--max", type=int, default=0, help="Exit after reading this many messages (0 = infinite)")
    p.add_argument("--delay", "-d", type=float, default=1.5, help="Delay in seconds between messages")
    p.add_argument("--model", "-m", default="fraud_model.pkl", help="Path to ML model pickle file")
    args = p.parse_args()

    print(f"Loading ML model from {args.model}...")
    predictor = FraudPredictor(args.model)
    print("✓ Model loaded!\n")

    consumer = KafkaConsumer(args.topic,
                             bootstrap_servers=[args.broker],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id=args.group,
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    count = 0
    agreements = 0
    disagreements = 0
    
    print("="*70)
    print("ML-ENHANCED FRAUD DETECTION CONSUMER")
    print("="*70)
    print("Showing: Flink Filter (actual) vs ML Model Prediction")
    print("="*70 + "\n")
    
    try:
        for msg in consumer:
            count += 1
            data = msg.value
            
            # Get ML prediction
            ml_prediction, ml_confidence = predictor.predict(data)
            
            # Flink already filtered these as fraud (is_fraudulent == 1)
            flink_says_fraud = data.get('is_fraudulent', 0) == 1
            ml_says_fraud = ml_prediction == 1
            
            # Track agreement
            if flink_says_fraud == ml_says_fraud:
                agreements += 1
                agreement = "✅ AGREE"
            else:
                disagreements += 1
                agreement = "⚠️  DISAGREE"
            
            print(f"\n{'='*70}")
            print(f"Message #{count} | {agreement}")
            print(f"{'='*70}")
            print(f"Click ID: {data.get('click_id', 'N/A')}")
            print(f"IP Address: {data.get('ip_address', 'N/A')}")
            print(f"Bot Score: {data.get('bot_likelihood_score', 0):.2f}")
            print(f"\n📊 VERDICT:")
            print(f"  Flink Filter:    {'🔴 FRAUD' if flink_says_fraud else '🟢 LEGITIMATE'}")
            print(f"  ML Prediction:   {'🔴 FRAUD' if ml_says_fraud else '🟢 LEGITIMATE'} (Confidence: {ml_confidence:.1%})")
            
            if count > 1:
                print(f"\n📈 STATS: Agreements: {agreements}/{count} ({agreements/count*100:.1f}%) | Disagreements: {disagreements}")
            
            if args.max and count >= args.max:
                break
            
            if args.delay > 0 and (not args.max or count < args.max):
                time.sleep(args.delay)
                
    except KeyboardInterrupt:
        print("\n\nStopping consumer...")
    finally:
        consumer.close()
        print(f"\n{'='*70}")
        print(f"FINAL STATS:")
        print(f"Total Messages: {count}")
        print(f"Agreements: {agreements} ({agreements/count*100:.1f}%)" if count > 0 else "")
        print(f"Disagreements: {disagreements} ({disagreements/count*100:.1f}%)" if count > 0 else "")
        print(f"{'='*70}")
