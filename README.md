⚡ Australian NEM Power & Emissions Dashboard
---

📌 Overview
This project delivers an end-to-end data pipeline and interactive visualization platform for the Australian National Electricity Market (NEM). It automates the collection, processing, and visualisation of real-time power generation and greenhouse gas emissions data from large-scale electricity facilities (>100 MW).

The system integrates live operational data with historical emissions records to provide meaningful, up-to-date insights into energy production and environmental impact across Australia.

---

🚀 Key Features
🔄 Real-Time Data Pipeline
Automated 5-minute interval data ingestion from the OpenElectricity API.

📡 MQTT Integration
Change-aware MQTT publishing to minimise network load and ensure efficient data transmission.

📊 Live Interactive Dashboard
Geospatial and temporal visualisations built with Dash and Plotly.

🕰️ Historical Data Integration
Seamless merging of live operational data with historical emissions datasets.

🤖 Autonomous Execution
Continuous runner with automatic error recovery and graceful shutdown handling.

---

🧠 System Architecture
The system consists of six integrated components:

- datapipeline.py
Interfaces with the OpenElectricity API to collect real-time generation data.

- dataintegration.py
Aggregates unit-level data into parent facility-level datasets.

- mqttpublisher.py
Manages efficient MQTT message publishing.

- continuousrunner.py
Controls the 60-second execution loop, error handling, and recovery.

- dashboard.py
Web-based dashboard for live data exploration and visualisation.

- preprocessass1.py
Prepares historical emissions data for longitudinal analysis.

---

🛠️ Getting Started
Prerequisites

- Python 3.x
- MQTT Broker (e.g., Mosquitto)
- Required Python packages:
  - pandas
  - dash
  - plotly
  - paho-mqtt
  - requests
  
---

📥 Installation
Clone the repository

git clone https://github.com/yourusername/nem-power-emissions-dashboard.git
cd nem-power-emissions-dashboard

---

Install dependencies
- pip install -r requirements.txt

---

▶️ Usage

- Start the MQTT broker
  mosquitto
  
- Run the continuous execution manager
  python continuousrunner.py

- Launch the dashboard
  python dashboard.py

Access the dashboard via your browser to explore live and historical insights.

---

👥 Team
Priyansh Khandelwal — Data Engineering & Pipeline Optimisation
Zhenzhe Wu — Visualisation & Historical Data Integration

---

📄 License
This project was developed for academic purposes as part of COMP5339.

---
