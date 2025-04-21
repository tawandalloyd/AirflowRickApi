# Project Title: Rick_and_Morty_ETL

## Description  
This project uses **Apache Airflow** running in a **Dockerized environment (Astro CLI)** to extract character and episode data from the **Rick and Morty API** using Python. The raw data is stored in **Amazon S3** in JSON format, transformed into **CSV**, and then re-uploaded to a separate S3 bucket. The pipeline includes Slack notifications for success and failure, and utilizes Airflow features like Sensors, XComs, and PythonOperators.

---

## Getting Started

### Prerequisites

- Docker installed locally
- Astro CLI installed ([Install Guide](https://docs.astronomer.io/astro/cli/install-cli))
- An AWS account with:
  - Access to create S3 buckets
- A Slack Webhook URL for notifications
- Rick and Morty public API (no authentication needed)

---

## Installing

1. **Clone the repository** to your local machine:
2. Use the Astro CLI to run the Airflow project usiong the commands below:
     - astro dev start: to run the containers and start the airflow project
     - astro dev stop: to stop the project


## Authors

Tawanda Charuka


