# Seattle Fire 911 Data Analytics

This repository contains data pipelines, transformations, and exploratory analysis for Seattle Fire 911 call data using Databricks and Python.

## Project Structure

- **explorations/**: Jupyter notebooks for data exploration and visualization.
- **ingestion/**: Scripts for ingesting raw data into Databricks tables.
- **transformations/**:
  - **bronze/**: Initial raw data transformations.
  - **silver/**: Cleaned and enriched data transformations.
  - **gold/**: Aggregated and business-ready data transformations.
- **typings/**: Type definitions and stubs.
- **utilities/**: Utility scripts and helper functions.

## Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/seattle_data_analytics.git
   cd seattle_data_analytics
   ```
2. **Set up your Python environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. **Configure Databricks connection**
   - Update `databricks.session.sql` and `databricks.yml` with your Databricks workspace details.

4. **Run the pipeline**
   - Use the scripts in `ingestion/` and `transformations/` to process data.
   - Explore data using notebooks in `explorations/`.

## Example Analyses
- Incident counts by month and type
- Geospatial density mapping of incidents
- Response time analysis (if available)
- Data quality checks

## Requirements
- Python 3.8+
- Databricks Connect
- pandas, matplotlib, plotly, etc.

## License
MIT License
