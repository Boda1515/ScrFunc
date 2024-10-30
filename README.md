# Azure-Scraping-Func
A Python-based scraper for collecting phone data from Amazon, with Azure pipelines for data storage and comparison.

## Introduction
This project is designed to scrape product data (phones) from Amazon, store it on Azure, and create pipelines to compare products in a data warehouse. The main goals include:

* Web scraping with CAPTCHA handling.
* Azure pipeline for data storage.
* Data comparison and analysis.

## Features
* Scrapes phone data dynamically from Amazon US, Egypt, and Japan.
* CAPTCHA detection and handling.
* Azure integration for data storage.
* Data pipeline for product comparison.

## Prerequisites
* Python 3.11
* Azure account with access to:
  ** Azure Functions
  ** Azure Data Lake

* GitHub for deployment integration
* Libraries: requests, beautifulsoup4, pandas, etc.
