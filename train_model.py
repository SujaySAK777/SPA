#!/usr/bin/env python3
"""
Train a Machine Learning model to detect click fraud.
Saves the trained model as a pickle file.
"""
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
import pickle
import argparse


def train_fraud_detection_model(csv_path, output_path='fraud_model.pkl'):
    """
    Train a Random Forest classifier on the click fraud dataset.
    
    Features used:
    - click_duration
    - scroll_depth
    - mouse_movement
    - keystrokes_detected
    - click_frequency
    - time_since_last_click
    - VPN_usage
    - proxy_usage
    - bot_likelihood_score
    - device_type (encoded)
    - browser (encoded)
    - operating_system (encoded)
    - device_ip_reputation (encoded)
    - ad_position (encoded)
    """
    print(f"Loading dataset from {csv_path}...")
    df = pd.read_csv(csv_path)
    
    print(f"Dataset shape: {df.shape}")
    print(f"Fraud cases: {df['is_fraudulent'].sum()} ({df['is_fraudulent'].mean()*100:.2f}%)")
    print(f"Non-fraud cases: {(df['is_fraudulent'] == 0).sum()} ({(1-df['is_fraudulent'].mean())*100:.2f}%)")
    
    # Select features for training
    numeric_features = [
        'click_duration', 'scroll_depth', 'mouse_movement', 
        'keystrokes_detected', 'click_frequency', 'time_since_last_click',
        'VPN_usage', 'proxy_usage', 'bot_likelihood_score'
    ]
    
    categorical_features = [
        'device_type', 'browser', 'operating_system', 
        'device_ip_reputation', 'ad_position'
    ]
    
    # Prepare data
    X = df[numeric_features + categorical_features].copy()
    y = df['is_fraudulent']
    
    # Encode categorical features
    label_encoders = {}
    for col in categorical_features:
        le = LabelEncoder()
        X[col] = le.fit_transform(X[col].astype(str))
        label_encoders[col] = le
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    print("\nTraining Random Forest model...")
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=15,
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1,
        class_weight='balanced'
    )
    
    model.fit(X_train, y_train)
    
    # Evaluate
    y_pred = model.predict(X_test)
    print("\n" + "="*50)
    print("MODEL EVALUATION")
    print("="*50)
    print(f"Accuracy: {accuracy_score(y_test, y_pred):.4f}")
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_pred))
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=['Non-Fraud', 'Fraud']))
    
    # Feature importance
    feature_importance = pd.DataFrame({
        'feature': numeric_features + categorical_features,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("\nTop 10 Most Important Features:")
    print(feature_importance.head(10).to_string(index=False))
    
    # Save model and encoders
    model_data = {
        'model': model,
        'label_encoders': label_encoders,
        'numeric_features': numeric_features,
        'categorical_features': categorical_features,
        'feature_names': numeric_features + categorical_features
    }
    
    with open(output_path, 'wb') as f:
        pickle.dump(model_data, f)
    
    print(f"\n✓ Model saved to {output_path}")
    return model, label_encoders, feature_importance


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Train fraud detection model')
    parser.add_argument('--file', '-f', default='click_fraud_dataset.csv', 
                        help='Path to CSV file')
    parser.add_argument('--output', '-o', default='fraud_model.pkl', 
                        help='Output path for pickle file')
    args = parser.parse_args()
    
    train_fraud_detection_model(args.file, args.output)
