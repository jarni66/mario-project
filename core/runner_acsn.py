"""
Process a single SEC accession: fetch 13F info table, parse (XML/HTML/LLM), upload parquet to Dropbox.
"""

import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
from datetime import datetime
import numpy as np
import re
import xml.etree.ElementTree as ET
from config import bq_dtype
import traceback
from utils import llm_parser
import asyncio
from pathlib import Path
import io

# Emails used to rotate User-Agent on each retry (SEC requires contact in user-agent).
EMAILS = [
    "nizar.rizax@gmail.com",
    "project.mario.1@example.com",
    "data.fetcher.pro@example.com",
    "research.bot@example.com",
]

HEADERS_BASE = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,ko;q=0.8,id;q=0.7,de;q=0.6,de-CH;q=0.5",
    "origin": "https://www.sec.gov",
    "priority": "u=1, i",
    "referer": "https://www.sec.gov/",
    "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
}

SEC_REQUEST_MAX_RETRIES = 5
SEC_REQUEST_SLEEP_INCREMENT = 10


def _headers_for_attempt(attempt_index: int) -> dict:
    h = HEADERS_BASE.copy()
    email = EMAILS[attempt_index % len(EMAILS)]
    h["user-agent"] = f"Mario bot project {email}"
    return h


def sec_get(url: str, max_retries: int = SEC_REQUEST_MAX_RETRIES) -> requests.Response:
    last_response = None
    last_exception = None
    for attempt in range(max_retries):
        if attempt > 0:
            sleep_sec = attempt * SEC_REQUEST_SLEEP_INCREMENT
            print(f"  Retry {attempt}/{max_retries - 1} after {sleep_sec}s (user-agent: {EMAILS[attempt % len(EMAILS)]})")
            time.sleep(sleep_sec)
        h = _headers_for_attempt(attempt)
        try:
            resp = requests.get(url, headers=h, timeout=60)
            last_response = resp
            if resp.status_code == 200:
                return resp
            if 400 <= resp.status_code < 500 and resp.status_code != 429:
                return resp
        except requests.RequestException as e:
            last_exception = e
    if last_response is not None:
        return last_response
    if last_exception is not None:
        raise last_exception
    return last_response


class ProcessACSN:
    def __init__(self, row: dict, dbx_manager):
        self.record = row
        self.acsn = self.record.get("accessionNumber")
        self.cik = str(self.record.get("cik")).zfill(10)
        self.info_table_records = []
        self.full_txt_url = f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{self.acsn.replace('-','')}/{self.acsn}.txt"
        rep_date = self.record.get("reportDate").replace("-", "_")
        self.file_prefix = f"{self.cik}_{self.acsn.replace('-','_')}_{rep_date}"
        self.dbx_manager = dbx_manager
        self.raw_path = f"Nizar/raw_txt/{rep_date}"
        os.makedirs(self.raw_path, exist_ok=True)
        file_name = self.file_prefix + ".parquet"
        self.op_path = f"/Nizar/sec_forms/{rep_date}/{file_name}"

    def save_data(self):
        col_names = [
            "name_of_issuer", "title_of_class", "cusip", "figi", "value",
            "shares_or_percent_amount", "shares_or_percent_type", "put_call",
            "investment_discretion", "other_manager", "voting_authority_sole",
            "voting_authority_shared", "voting_authority_none",
        ]
        rep_date = self.record.get("reportDate").replace("-", "_")
        file_name = self.file_prefix + ".parquet"
        dropbox_op_path = f"/Nizar/sec_forms/{rep_date}/{file_name}"
        df = pd.DataFrame(self.info_table_records, columns=col_names, dtype=str)
        df = df.replace("", np.nan).infer_objects(copy=False)
        df = df.dropna(subset=["name_of_issuer"])
        df["business_address"] = self.record.get("business_address")
        df["mailing_address"] = self.record.get("mailing_address")
        df["report_date"] = self.record.get("reportDate")
        df["filling_date"] = self.record.get("filingDate")
        df["acsn"] = self.record.get("accessionNumber")
        df["cik"] = self.record.get("cik")
        df["value_multiplies"] = self.record.get("value_multiplies")
        df["scraping_url"] = self.full_txt_url
        df["scraping_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            cols_to_clean = [
                "value", "shares_or_percent_amount", "voting_authority_sole",
                "voting_authority_shared", "voting_authority_none",
            ]
            for col in cols_to_clean:
                if col in df.columns:
                    df[col] = df[col].str.replace(",", "")
            for column, dtype in bq_dtype.items():
                if column not in df.columns:
                    df[column] = np.nan
            try:
                df = df.astype(bq_dtype)
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)
                print(f"Uploading to Dropbox: {dropbox_op_path} ({len(df)} rows)")
                upload_result = self.dbx_manager.upload_stream(parquet_buffer.getvalue(), dropbox_op_path)
                if upload_result:
                    self.record["upload_to_bucket_status"] = "Success"
                    self.record["length"] = len(df)
                    self.record["table_path"] = dropbox_op_path
                else:
                    raise Exception("Dropbox upload returned None")
            except Exception as e:
                err = traceback.format_exc()
                print(f"Failed to upload Parquet: {err}")
                self.record["upload_to_bucket_status"] = f"Error: {str(e)}"
                self.record["table_path"] = dropbox_op_path
        except Exception as e:
            err = traceback.format_exc()
            error_msg = f"Error in save_data : {str(err)}"
            print(error_msg)
            try:
                csv_buffer = io.BytesIO()
                df.to_csv(csv_buffer, index=False)
                failed_path = f"/Nizar/failed/{file_name.replace('.parquet', '.csv')}"
                self.dbx_manager.upload_stream(csv_buffer.getvalue(), failed_path)
                self.record["table_path"] = failed_path
            except Exception:
                print("Could not even save fallback CSV to Dropbox.")
            self.record["upload_to_bucket_status"] = error_msg

    def check_value_multiplies(self, url):
        response = sec_get(url)
        if response.status_code == 200:
            text_body = response.text
            match = re.search(r"x\$(\d+)", text_body)
            if match:
                return int(match.group(1))
            if "[thousands]" in text_body.lower():
                return 1000
            return 1
        print("Check value multiplies", response.status_code)
        return 1

    def get_html_info_table(self):
        url = f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{self.acsn.replace('-','')}/{self.acsn}-index.html"
        response = sec_get(url)
        result = {"info_html": "", "complete_txt": ""}
        if response.status_code == 200:
            text_body = response.text
            soup = BeautifulSoup(text_body, "html.parser")
            table = soup.find("table")
            if table:
                all_tr = table.find_all("tr")
                for tr in all_tr:
                    a_tag = tr.find("a")
                    if a_tag:
                        tr_str = str(tr).lower()
                        if ("table" in tr_str) and (".html" in a_tag.text):
                            result["info_html"] = "https://www.sec.gov" + a_tag.attrs.get("href", "")
                a_tag_last = all_tr[-1].find("a")
                if a_tag_last:
                    result["complete_txt"] = "https://www.sec.gov" + a_tag_last.attrs.get("href", "")
        else:
            print("Get html info table url", response.status_code)
        return result

    def get_parser(self, req_text):
        records = [
            self.parser(req_text),
            self.parser2(req_text),
            self.parser3(req_text),
        ]
        result = []
        for i in range(len(records)):
            if len(records[i]) != 0:
                self.record["parser"] = i + 1
                result = records[i]
                print(f"Parser {i+1} : ", len(records[i]))
                break
        if result:
            return result
        self.record["parser"] = 4
        return self.parser_llm(req_text)

    def parser(self, req_text):
        match = re.search(r"(<[\w:]*informationTable[\s\S]*?</[\w:]*informationTable>)", req_text)
        if not match:
            return []
        xml_block = match.group(1)
        root = ET.fromstring(xml_block)

        def get_namespace(tag):
            if tag[0] == "{":
                return tag[1:].split("}")[0]
            return ""

        ns_uri = get_namespace(root.tag)
        ns = {"ns": ns_uri} if ns_uri else {}

        def ns_tag(tag_name):
            return f"ns:{tag_name}" if ns else tag_name

        data = []
        for info in root.findall(ns_tag("infoTable"), ns):
            entry = {
                "name_of_issuer": info.findtext(ns_tag("nameOfIssuer"), default="", namespaces=ns).strip(),
                "title_of_class": info.findtext(ns_tag("titleOfClass"), default="", namespaces=ns).strip(),
                "cusip": info.findtext(ns_tag("cusip"), default="", namespaces=ns).strip(),
                "figi": info.findtext(ns_tag("figi"), default="", namespaces=ns).strip(),
                "value": info.findtext(ns_tag("value"), default="", namespaces=ns).strip(),
                "shares_or_percent_amount": info.find(ns_tag("shrsOrPrnAmt") + "/" + ns_tag("sshPrnamt"), ns).text.strip() if info.find(ns_tag("shrsOrPrnAmt") + "/" + ns_tag("sshPrnamt"), ns) is not None else "",
                "shares_or_percent_type": info.find(ns_tag("shrsOrPrnAmt") + "/" + ns_tag("sshPrnamtType"), ns).text.strip() if info.find(ns_tag("shrsOrPrnAmt") + "/" + ns_tag("sshPrnamtType"), ns) is not None else "",
                "put_call": info.findtext(ns_tag("putCall"), default="", namespaces=ns).strip(),
                "investment_discretion": info.findtext(ns_tag("investmentDiscretion"), default="", namespaces=ns).strip(),
                "other_manager": info.findtext(ns_tag("otherManager"), default="", namespaces=ns).strip(),
                "voting_authority_sole": info.findtext(ns_tag("votingAuthority") + "/" + ns_tag("Sole"), default="0", namespaces=ns).strip(),
                "voting_authority_shared": info.findtext(ns_tag("votingAuthority") + "/" + ns_tag("Shared"), default="0", namespaces=ns).strip(),
                "voting_authority_none": info.findtext(ns_tag("votingAuthority") + "/" + ns_tag("None"), default="0", namespaces=ns).strip(),
            }
            for k in ("value", "shares_or_percent_amount", "voting_authority_sole", "voting_authority_shared", "voting_authority_none"):
                entry[k] = entry[k].replace(",", "")
            data.append(entry)
        return data

    def parser2(self, req_text):
        records = []
        try:
            table_texts = re.findall(r"<table>(.*?)</table>", req_text, flags=re.S | re.I)
            if not table_texts:
                return []
            for table_str in table_texts:
                if "issuer" in table_str.lower():
                    lines = table_str.strip("\n").splitlines()
                    header_index = None
                    for i, line in enumerate(lines):
                        if "<c>" in line.strip().lower():
                            header_index = i
                            lines[i] = line
                            break
                    if header_index is None:
                        raise ValueError("No header line with <S> found.")
                    table_lines = lines[header_index:]
                    header_line = table_lines[0]
                    col_positions = [m.start() for m in re.finditer("<", header_line)]
                    data_lines = table_lines[1:]
                    max_len = max(len(line) for line in data_lines)
                    threshold = max_len * 0.3
                    data_lines = [line for line in data_lines if len(line.strip()) >= threshold]
                    data_lines = [line.ljust(max_len) for line in data_lines]
                    entry_list = [
                        "name_of_issuer", "title_of_class", "cusip", "value",
                        "shares_or_percent_amount", "shares_or_percent_type", "put_call",
                        "investment_discretion", "other_manager",
                        "voting_authority_sole", "voting_authority_shared", "voting_authority_none",
                    ]
                    for row in data_lines:
                        if not row.strip():
                            continue
                        entry = {k: "0" if "authority" in k or k in ("value", "shares_or_percent_amount") else "" for k in entry_list}
                        entry["figi"] = ""
                        row += "     "
                        for i, start in enumerate(col_positions):
                            try:
                                left = start
                                if left > 0 and not row[left].isspace():
                                    while left > 0 and not row[left - 1].isspace():
                                        left -= 1
                                right = col_positions[i + 1] if i + 1 < len(col_positions) else len(row)
                                if i + 1 < len(col_positions):
                                    while right > left and not row[right].isspace():
                                        right -= 1
                                value = row[left:right].strip()
                                entry[entry_list[i]] = value
                            except Exception:
                                pass
                        for k in ("value", "shares_or_percent_amount", "voting_authority_sole", "voting_authority_shared", "voting_authority_none"):
                            v = entry.get(k, "0").replace(",", "").strip() or "0"
                            entry[k] = float(v)
                        if entry["voting_authority_sole"] + entry["voting_authority_shared"] + entry["voting_authority_none"] != 0:
                            records.append(entry)
        except Exception:
            pass
        print("Parser 2 len :", len(records))
        return records

    def parser3(self, req_text):
        records = []
        try:
            table_texts = re.findall(r"<table>(.*?)</table>", req_text, flags=re.S | re.I)
            if not table_texts:
                return []
            for table_str in table_texts:
                if "issuer" in table_str.lower():
                    lines = table_str.strip("\n").splitlines()
                    header_index = None
                    for i, line in enumerate(lines):
                        if "<c>" in line.strip().lower():
                            header_index = i
                            lines[i] = line + "  <C>"
                            break
                    if header_index is None:
                        raise ValueError("No header line with <S> found.")
                    table_lines = lines[header_index:]
                    data_lines = table_lines[1:]
                    max_len = max(len(line) for line in data_lines)
                    threshold = max_len * 0.3
                    data_lines = [line for line in data_lines if len(line.strip()) >= threshold]
                    data_lines = [line.ljust(max_len) for line in data_lines]

                    def _strip_wrapping_quotes(line: str) -> str:
                        ln = line.strip()
                        if ln.startswith('"') and ln.endswith('",'):
                            return ln[1:-2].rstrip()
                        if ln.startswith('"') and ln.endswith('"'):
                            return ln[1:-1]
                        return ln

                    def parse_line(line: str) -> dict:
                        ln = _strip_wrapping_quotes(line)
                        m = re.match(r"^\s*(?P<name>.+?)\s{2,}(?P<title>.+?)\s{2,}(?P<cusip>\S+)\s*(?P<rest>.*)$", ln)
                        if not m:
                            raise ValueError(f"Line didn't match: {line!r}")
                        name = m.group("name").strip()
                        title = m.group("title").strip()
                        cusip = m.group("cusip").strip()
                        rest = m.group("rest")
                        m2 = re.match(r"^\s*([\d,]+)?\s*([\d,]+)?\s*(.*)$", rest)
                        value = (m2.group(1) or "").strip()
                        shares_amt = (m2.group(2) or "").strip()
                        tail = m2.group(3)
                        numeric_tokens = re.findall(r"[\d,]+", tail)
                        if numeric_tokens:
                            tail = re.sub(r"(?:\s+[\d,]+){%d}\s*$" % len(numeric_tokens), "", tail)
                        text_part = tail.strip()
                        shares_or_percent_type = ""
                        put_call = ""
                        investment_discretion = ""
                        if text_part:
                            tokens = text_part.split()
                            for t in tokens:
                                up = t.upper()
                                if not shares_or_percent_type and up in {"SH", "PRN", "SHARES", "PRF"}:
                                    shares_or_percent_type = t
                                    continue
                                if not put_call and up in {"PUT", "CALL", "P", "C"}:
                                    put_call = t
                                    continue
                                investment_discretion = (investment_discretion + " " + t) if investment_discretion else t
                        other_manager = ""
                        voting_authority_sole = ""
                        voting_authority_shared = ""
                        voting_authority_none = ""
                        if numeric_tokens:
                            voting_authority_none = numeric_tokens[-1]
                        if len(numeric_tokens) >= 2:
                            voting_authority_shared = numeric_tokens[-2]
                        if len(numeric_tokens) >= 3:
                            voting_authority_sole = numeric_tokens[-3]
                        if len(numeric_tokens) >= 4:
                            other_manager = numeric_tokens[-4]
                        return {
                            "name_of_issuer": name, "title_of_class": title, "cusip": cusip,
                            "value": value, "shares_or_percent_amount": shares_amt,
                            "shares_or_percent_type": shares_or_percent_type, "put_call": put_call,
                            "investment_discretion": investment_discretion, "other_manager": other_manager,
                            "voting_authority_sole": voting_authority_sole, "voting_authority_shared": voting_authority_shared,
                            "voting_authority_none": voting_authority_none,
                        }

                    for line in data_lines:
                        try:
                            entry = parse_line(line)
                            if entry.get("name_of_issuer", "").lower() == "total":
                                continue
                            entry["figi"] = ""
                            for k in ("value", "shares_or_percent_amount", "voting_authority_sole", "voting_authority_shared", "voting_authority_none"):
                                v = entry.get(k, "0").replace(",", "").strip() or "0"
                                entry[k] = float(v)
                            if entry["voting_authority_sole"] + entry["voting_authority_shared"] + entry["voting_authority_none"] != 0:
                                records.append(entry)
                        except Exception:
                            pass
        except Exception:
            pass
        return records

    def parser_llm(self, req_text):
        table_res = []
        table_texts = re.findall(r"<table>(.*?)</table>", req_text, flags=re.S | re.I)
        if table_texts:
            for table_str in table_texts:
                if ("issuer" in table_str.lower()) or ("cusip" in table_str.lower()):
                    print("Running extract llm")
                    try:
                        extract_res = asyncio.run(llm_parser.get_records(table_str))
                        table_res += extract_res
                    except Exception as e:
                        print("Error llm parser :", e)
        elif ("issuer" in req_text.lower()) or ("cusip" in req_text.lower()):
            print("Running extract llm")
            try:
                extract_res = asyncio.run(llm_parser.get_records(req_text))
                table_res += extract_res
            except Exception as e:
                print("Error llm parser :", e)

        for entry in table_res:
            for k in ("value", "shares_or_percent_amount", "voting_authority_sole", "voting_authority_shared", "voting_authority_none"):
                v = str(entry.get(k, "0")).replace(",", "").strip() or "0"
                entry[k] = float(v)
        return table_res

    def get_info_table(self):
        self.record["raw_txt"] = ""
        response = sec_get(self.full_txt_url)
        if response.status_code == 200:
            text_body = response.text
            self.record["raw_txt"] = f"{self.raw_path}/{self.file_prefix}.txt"
            return self.get_parser(text_body)
        print("Get info table full text", response.status_code)
        return []

    def run(self):
        print("Process :", self.cik, self.acsn)
        self.record["bq_status"] = "process"
        try:
            index_res = self.get_html_info_table()
            if index_res.get("complete_txt"):
                self.full_txt_url = index_res.get("complete_txt")
            url_html_info_table = index_res.get("info_html")
            if url_html_info_table:
                self.record["url_html_info_table"] = url_html_info_table
                self.record["value_multiplies"] = self.check_value_multiplies(url_html_info_table)
            else:
                self.record["url_html_info_table"] = ""
                self.record["value_multiplies"] = self.check_value_multiplies(self.full_txt_url)
            self.info_table_records = self.get_info_table()
            self.record["info_table_len"] = len(self.info_table_records)
            self.save_data()
            self.record["process"] = ""
        except Exception as e:
            print("error process", e)
            self.record["process"] = str(traceback.format_exc())

    def run_check_multiplies(self):
        index_res = self.get_html_info_table()
        if index_res.get("complete_txt"):
            self.full_txt_url = index_res.get("complete_txt")
        url_html_info_table = index_res.get("info_html")
        if url_html_info_table:
            self.record["url_html_info_table"] = url_html_info_table
            self.record["value_multiplies"] = self.check_value_multiplies(url_html_info_table)
        else:
            self.record["url_html_info_table"] = ""
            self.record["value_multiplies"] = self.check_value_multiplies(self.full_txt_url)

    def run_acsn(self):
        try:
            index_res = self.get_html_info_table()
            if index_res.get("complete_txt"):
                self.full_txt_url = index_res.get("complete_txt")
            url_html_info_table = index_res.get("info_html")
            if url_html_info_table:
                self.record["url_html_info_table"] = url_html_info_table
                self.record["value_multiplies"] = self.check_value_multiplies(url_html_info_table)
            else:
                self.record["url_html_info_table"] = ""
                self.record["value_multiplies"] = self.check_value_multiplies(self.full_txt_url)
            self.info_table_records = self.get_info_table()
            self.record["info_table_len"] = len(self.info_table_records)
            self.save_data()
            self.record["process"] = ""
        except Exception as e:
            self.record["process"] = str(traceback.format_exc())
            print("error process", traceback.format_exc())
