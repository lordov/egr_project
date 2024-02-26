
EGR Data Scraper
Overview
This Python script is designed to scrape data from the Electronic Government Register (EGR) of Belarus using the provided API. The script retrieves information about companies based on their Unique Payer Number (UNP). The collected data includes company names, activities, contact information, and addresses.

Features
Data Retrieval: The script makes API requests to obtain company names, activities, and detailed information from the EGR.
Database Integration: Collected data is stored in a SQLite database named my_database.db.
Error Handling: The script handles API rate-limiting errors, logs unexpected errors, and retries failed requests after a delay.
Logging: Detailed logs are stored in the egr_data.log file, providing insights into the script's execution.
Requirements
Python 3.x: Make sure you have Python 3.x installed.
Libraries: Install the required libraries using pip install requests urllib3.
Usage
Clone the repository:

```bash
Copy code
git clone https://github.com/your-username/egr-data-scraper.git
Navigate to the project directory:
```


```bash
Copy code
cd egr-data-scraper
Run the script:
```

```bash
Copy code
python egr.py
```
The script will generate and store company data in the SQLite database.

Configuration
API Endpoint: The script uses the EGR API with the following endpoints:

Company Name: https://egr.gov.by/api/v2/egr/getJurNamesByRegNum/{unp}  
Company Activity: https://egr.gov.by/api/v2/egr/getVEDByRegNum/{unp}  
Company Information: https://egr.gov.by/api/v2/egr/getAddressByRegNum/{unp}  

Database: The script uses SQLite to store data. You can customize the database settings in the create_table() and insert_data() functions.

Disclaimer
This script is for educational and informational purposes only. Use it responsibly and ensure compliance with the EGR API terms of service.

Feel free to customize and enhance the script based on your needs. If you encounter any issues or have suggestions, please open an issue or submit a pull request. Happy scraping!
