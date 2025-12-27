#!/usr/bin/env python3
"""
Streamlit Dashboard for Real-Time Click Fraud Detection
Displays streaming data, ML predictions, and analytics
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import time
from collections import deque
import threading
from kafka import KafkaConsumer
import pickle

# Page configuration
st.set_page_config(
    page_title="Click Fraud Detection Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .fraud-alert {
        background-color: #ff4444;
        color: white;
        padding: 1rem;
        border-radius: 10px;
        font-weight: bold;
    }
    .safe-alert {
        background-color: #00C851;
        color: white;
        padding: 1rem;
        border-radius: 10px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)


class KafkaStreamConsumer:
    """Background thread to consume Kafka messages and store in shared buffer"""
    
    def __init__(self, broker, topic, max_buffer=1000):
        self.broker = broker
        self.topic = topic
        self.buffer = deque(maxlen=max_buffer)
        self.running = False
        self.thread = None
        self.stats = {
            'total_clicks': 0,
            'fraud_detected': 0,
            'legitimate_clicks': 0,
            'total_probability': 0.0,
        }
        self.model_data = None
        
    def load_model(self, model_path='fraud_model.pkl'):
        """Load the trained ML model"""
        try:
            with open(model_path, 'rb') as f:
                self.model_data = pickle.load(f)
            return True
        except Exception as e:
            st.error(f"Failed to load model: {e}")
            return False
    
    def predict_fraud(self, data):
        """Predict fraud using the loaded ML model"""
        if not self.model_data:
            return None
            
        try:
            import pandas as pd
            
            model = self.model_data['model']
            label_encoders = self.model_data['label_encoders']
            numeric_features = self.model_data['numeric_features']
            categorical_features = self.model_data['categorical_features']
            
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
                'legitimate_probability': float(probability[0]),
                'confidence': float(max(probability))
            }
        except Exception as e:
            return None
    
    def start(self):
        """Start consuming messages in background thread"""
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._consume, daemon=True)
        self.thread.start()
    
    def _consume(self):
        """Internal method to consume messages"""
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=[self.broker],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='streamlit-dashboard-group',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            
            while self.running:
                for msg in consumer:
                    if not self.running:
                        break
                    
                    data = msg.value
                    
                    # Get ML prediction
                    ml_result = self.predict_fraud(data)
                    if ml_result:
                        data['ml_prediction'] = ml_result['prediction']
                        data['ml_fraud_probability'] = ml_result['fraud_probability']
                        data['ml_confidence'] = ml_result['confidence']
                    
                    # Add timestamp if not present
                    if 'processed_time' not in data:
                        data['processed_time'] = datetime.now().isoformat()
                    
                    self.buffer.append(data)
                    
                    # Update stats
                    self.stats['total_clicks'] += 1
                    if ml_result and ml_result['prediction'] == 1:
                        self.stats['fraud_detected'] += 1
                    else:
                        self.stats['legitimate_clicks'] += 1
                    
                    if ml_result:
                        self.stats['total_probability'] += ml_result['fraud_probability']
                    
        except Exception as e:
            st.error(f"Kafka consumer error: {e}")
    
    def stop(self):
        """Stop consuming messages"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)
    
    def get_recent_data(self, n=100):
        """Get the most recent n messages"""
        return list(self.buffer)[-n:] if self.buffer else []
    
    def get_stats(self):
        """Get current statistics"""
        return self.stats.copy()


# Initialize session state
if 'kafka_consumer' not in st.session_state:
    st.session_state.kafka_consumer = None
    st.session_state.connected = False


def init_kafka_connection(broker, topic, model_path):
    """Initialize Kafka connection"""
    consumer = KafkaStreamConsumer(broker, topic)
    
    # Load model
    if consumer.load_model(model_path):
        consumer.start()
        return consumer
    return None


def main():
    # Header
    st.markdown('<div class="main-header">🔍 Real-Time Click Fraud Detection Dashboard</div>', unsafe_allow_html=True)
    
    # Sidebar Configuration
    st.sidebar.title("⚙️ Configuration")
    
    broker = st.sidebar.text_input("Kafka Broker", value="localhost:9092")
    topic = st.sidebar.text_input("Kafka Topic", value="input-topic")
    model_path = st.sidebar.text_input("Model Path", value="fraud_model.pkl")
    refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 1, 10, 2)
    max_display = st.sidebar.slider("Max Records to Display", 10, 500, 100)
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 💡 Topic Selection")
    st.sidebar.info("**input-topic**: All clicks with ML predictions\n\n**output-topic**: Only fraudulent clicks (filtered by Flink)")
    
    # Connect/Disconnect button
    if not st.session_state.connected:
        if st.sidebar.button("🔌 Connect to Stream"):
            with st.spinner("Connecting to Kafka..."):
                consumer = init_kafka_connection(broker, topic, model_path)
                if consumer:
                    st.session_state.kafka_consumer = consumer
                    st.session_state.connected = True
                    st.sidebar.success("✅ Connected!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.sidebar.error("❌ Failed to connect")
    else:
        if st.sidebar.button("🔌 Disconnect"):
            if st.session_state.kafka_consumer:
                st.session_state.kafka_consumer.stop()
            st.session_state.kafka_consumer = None
            st.session_state.connected = False
            st.sidebar.info("Disconnected")
            time.sleep(1)
            st.rerun()
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Dashboard Info")
    st.sidebar.info("This dashboard displays real-time click fraud detection results using ML predictions from streaming data.")
    
    # Main content
    if not st.session_state.connected or not st.session_state.kafka_consumer:
        st.info("👆 Please configure and connect to Kafka stream in the sidebar to start viewing data.")
        
        # Show sample data structure
        st.subheader("📋 Expected Data Structure")
        sample_data = {
            "click_id": "d875835d-3a4a-4a20-b0d1-6cddf89afc6a",
            "timestamp": "2024-08-23 02:47:39",
            "user_id": "65a2f621-707b-49be-9c3e-ccac0b1d89ef",
            "ip_address": "141.36.49.37",
            "device_type": "Tablet",
            "browser": "Safari",
            "click_duration": 0.29,
            "is_fraudulent": 0,
            "ml_prediction": 0,
            "ml_fraud_probability": 0.15
        }
        st.json(sample_data)
        return
    
    # Get data from Kafka consumer
    consumer = st.session_state.kafka_consumer
    recent_data = consumer.get_recent_data(max_display)
    stats = consumer.get_stats()
    
    # Auto-refresh
    placeholder = st.empty()
    
    with placeholder.container():
        # Key Metrics Row
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Total Clicks", stats['total_clicks'])
        
        with col2:
            st.metric("🚨 Fraud Detected", stats['fraud_detected'], 
                     delta=f"{(stats['fraud_detected']/max(stats['total_clicks'], 1)*100):.1f}%")
        
        with col3:
            st.metric("✅ Legitimate", stats['legitimate_clicks'],
                     delta=f"{(stats['legitimate_clicks']/max(stats['total_clicks'], 1)*100):.1f}%")
        
        with col4:
            avg_fraud_prob = stats['total_probability'] / max(stats['total_clicks'], 1) * 100
            st.metric("Avg Fraud Probability", f"{avg_fraud_prob:.2f}%")
        
        with col5:
            fraud_rate = stats['fraud_detected'] / max(stats['total_clicks'], 1) * 100
            st.metric("Fraud Rate", f"{fraud_rate:.2f}%")
        
        st.markdown("---")
        
        if len(recent_data) > 0:
            # Create DataFrame
            df = pd.DataFrame(recent_data)
            
            # Two column layout for charts
            col_left, col_right = st.columns(2)
            
            with col_left:
                st.subheader("📊 Fraud Detection Distribution")
                if 'ml_prediction' in df.columns:
                    fraud_counts = df['ml_prediction'].value_counts()
                    fig_pie = go.Figure(data=[go.Pie(
                        labels=['Legitimate', 'Fraud'],
                        values=[fraud_counts.get(0, 0), fraud_counts.get(1, 0)],
                        hole=.3,
                        marker_colors=['#00C851', '#ff4444']
                    )])
                    fig_pie.update_layout(height=300)
                    st.plotly_chart(fig_pie, use_container_width=True)
            
            with col_right:
                st.subheader("📈 Fraud Probability Distribution")
                if 'ml_fraud_probability' in df.columns:
                    fig_hist = px.histogram(
                        df, 
                        x='ml_fraud_probability',
                        nbins=20,
                        title="Distribution of Fraud Probabilities",
                        color_discrete_sequence=['#667eea']
                    )
                    fig_hist.update_layout(height=300)
                    st.plotly_chart(fig_hist, use_container_width=True)
            
            # Device Type Analysis
            st.subheader("💻 Fraud by Device Type")
            if 'device_type' in df.columns and 'ml_prediction' in df.columns:
                device_fraud = df.groupby(['device_type', 'ml_prediction']).size().reset_index(name='count')
                fig_device = px.bar(
                    device_fraud,
                    x='device_type',
                    y='count',
                    color='ml_prediction',
                    barmode='group',
                    labels={'ml_prediction': 'Prediction', 'count': 'Count'},
                    color_discrete_map={0: '#00C851', 1: '#ff4444'}
                )
                st.plotly_chart(fig_device, use_container_width=True)
            
            # Recent Clicks Table
            st.subheader("🔄 Recent Click Events")
            
            # Display options
            show_fraud_only = st.checkbox("Show Fraud Only", value=False)
            
            if show_fraud_only and 'ml_prediction' in df.columns:
                display_df = df[df['ml_prediction'] == 1]
            else:
                display_df = df
            
            # Select relevant columns for display
            display_columns = ['click_id', 'timestamp', 'device_type', 'browser', 
                              'ip_address', 'click_duration', 'ml_prediction', 
                              'ml_fraud_probability', 'ml_confidence']
            display_columns = [col for col in display_columns if col in display_df.columns]
            
            # Format the dataframe
            if len(display_df) > 0:
                display_table = display_df[display_columns].tail(50).copy()
                
                # Apply styling
                def highlight_fraud(row):
                    if 'ml_prediction' in row and row['ml_prediction'] == 1:
                        return ['background-color: #ffcccc'] * len(row)
                    return [''] * len(row)
                
                styled_df = display_table.style.apply(highlight_fraud, axis=1)
                st.dataframe(styled_df, use_container_width=True, height=400)
            else:
                st.info("No data matching the filter criteria")
            
            # Latest Event Alert
            if len(display_df) > 0:
                latest = display_df.iloc[-1]
                st.subheader("🔔 Latest Event")
                
                if latest.get('ml_prediction') == 1:
                    st.markdown('<div class="fraud-alert">⚠️ FRAUD DETECTED!</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="safe-alert">✅ Legitimate Click</div>', unsafe_allow_html=True)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write(f"**Click ID:** {latest.get('click_id', 'N/A')}")
                    st.write(f"**Device:** {latest.get('device_type', 'N/A')}")
                    st.write(f"**Browser:** {latest.get('browser', 'N/A')}")
                
                with col2:
                    st.write(f"**IP Address:** {latest.get('ip_address', 'N/A')}")
                    st.write(f"**Timestamp:** {latest.get('timestamp', 'N/A')}")
                    st.write(f"**Duration:** {latest.get('click_duration', 'N/A')}s")
                
                with col3:
                    st.write(f"**Fraud Probability:** {latest.get('ml_fraud_probability', 0)*100:.2f}%")
                    st.write(f"**Confidence:** {latest.get('ml_confidence', 0)*100:.2f}%")
                    st.write(f"**Prediction:** {'FRAUD' if latest.get('ml_prediction') == 1 else 'LEGITIMATE'}")
        
        else:
            st.info("⏳ Waiting for streaming data... Make sure the producer is running.")
    
    # Auto-refresh
    time.sleep(refresh_rate)
    st.rerun()


if __name__ == "__main__":
    main()
