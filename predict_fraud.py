#!/usr/bin/env python3
"""
Load the trained ML model and make fraud predictions on streaming data.
"""
import pickle
import argparse
import json


class FraudPredictor:
    def __init__(self, model_path='fraud_model.pkl'):
        """Load the trained model and encoders."""
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model = model_data['model']
        self.label_encoders = model_data['label_encoders']
        self.numeric_features = model_data['numeric_features']
        self.categorical_features = model_data['categorical_features']
        self.feature_names = model_data['feature_names']
    
    def prepare_features(self, data):
        """
        Prepare features from JSON data for prediction.
        
        Args:
            data (dict): JSON data with click information
        
        Returns:
            list: Feature vector for prediction
        """
        features = []
        
        # Numeric features
        for feature in self.numeric_features:
            value = data.get(feature, 0)
            if value is None:
                value = 0
            features.append(float(value))
        
        # Categorical features (encoded)
        for feature in self.categorical_features:
            value = str(data.get(feature, 'Unknown'))
            
            # Handle unknown categories
            if feature in self.label_encoders:
                le = self.label_encoders[feature]
                try:
                    encoded_value = le.transform([value])[0]
                except ValueError:
                    # Use the most common class (0) for unknown values
                    encoded_value = 0
            else:
                encoded_value = 0
            
            features.append(encoded_value)
        
        return features
    
    def predict(self, data):
        """
        Predict if a click is fraudulent.
        
        Args:
            data (dict): JSON data with click information
        
        Returns:
            tuple: (prediction, probability) where prediction is 0 or 1
        """
        features = self.prepare_features(data)
        prediction = self.model.predict([features])[0]
        probability = self.model.predict_proba([features])[0]
        
        return int(prediction), float(probability[1])
    
    def predict_batch(self, data_list):
        """
        Predict fraud for multiple clicks.
        
        Args:
            data_list (list): List of JSON data dictionaries
        
        Returns:
            list: List of (prediction, probability) tuples
        """
        features_batch = [self.prepare_features(data) for data in data_list]
        predictions = self.model.predict(features_batch)
        probabilities = self.model.predict_proba(features_batch)
        
        return [(int(pred), float(prob[1])) for pred, prob in zip(predictions, probabilities)]


def interactive_mode(predictor):
    """Interactive mode for testing predictions."""
    print("\n" + "="*60)
    print("FRAUD PREDICTION - INTERACTIVE MODE")
    print("="*60)
    print("Enter click data as JSON or type 'quit' to exit")
    print("Example: {\"click_duration\": 0.5, \"bot_likelihood_score\": 0.9, ...}")
    print("="*60 + "\n")
    
    while True:
        try:
            user_input = input("Enter JSON data: ").strip()
            if user_input.lower() in ['quit', 'exit', 'q']:
                break
            
            data = json.loads(user_input)
            prediction, probability = predictor.predict(data)
            
            print(f"\n{'🔴 FRAUD' if prediction == 1 else '🟢 LEGITIMATE'}")
            print(f"Fraud Probability: {probability:.2%}")
            print(f"Prediction: {prediction}\n")
            
        except json.JSONDecodeError:
            print("❌ Invalid JSON format. Please try again.\n")
        except KeyboardInterrupt:
            print("\n\nExiting...")
            break
        except Exception as e:
            print(f"❌ Error: {e}\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Predict fraud using trained model')
    parser.add_argument('--model', '-m', default='fraud_model.pkl', 
                        help='Path to trained model pickle file')
    parser.add_argument('--interactive', '-i', action='store_true',
                        help='Run in interactive mode for testing')
    parser.add_argument('--test-data', '-t', type=str,
                        help='JSON string or file with test data')
    args = parser.parse_args()
    
    print(f"Loading model from {args.model}...")
    predictor = FraudPredictor(args.model)
    print("✓ Model loaded successfully!\n")
    
    if args.interactive:
        interactive_mode(predictor)
    elif args.test_data:
        try:
            data = json.loads(args.test_data)
            prediction, probability = predictor.predict(data)
            print(f"Prediction: {'FRAUD' if prediction == 1 else 'LEGITIMATE'}")
            print(f"Fraud Probability: {probability:.2%}")
        except Exception as e:
            print(f"Error: {e}")
    else:
        print("Use --interactive for testing or --test-data for single prediction")
        print(f"Example: python {__file__} --interactive")
