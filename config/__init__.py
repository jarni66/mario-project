"""
Mario project configuration: BigQuery dtypes and shared settings.
"""

BUCKET = "financial_data_nizar"
CIK_PATH = "run_log/BATCH_LOG/all_cik.json"

bq_dtype = {
    "scraping_url": "str",
    "scraping_timestamp": "datetime64[ns]",
    "cik": "str",
    "business_address": "str",
    "mailing_address": "str",
    "report_date": "datetime64[ns]",
    "filling_date": "datetime64[ns]",
    "name_of_issuer": "str",
    "title_of_class": "str",
    "cusip": "str",
    "value": "float64",
    "figi": "str",
    "put_call": "str",
    "shares_or_percent_amount": "float64",
    "shares_or_percent_type": "str",
    "investment_discretion": "str",
    "other_manager": "str",
    "voting_authority_sole": "float64",
    "voting_authority_shared": "float64",
    "voting_authority_none": "float64",
    "acsn": "str",
    "value_multiplies": "int",
}
