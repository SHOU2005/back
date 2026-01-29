"""
Excel Processor - Universal Transaction Extractor
Supports ANY Indian bank Excel statement (XLS/XLSX)
"""

import io
import logging
import re
from datetime import datetime
from typing import Dict, Any, List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ============================================================
# HELPERS
# ============================================================

def safe_float(val, default=0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def normalize_text(text) -> str:
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    text = str(text).upper()
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s/@.-]', ' ', text)
    return text.strip()


# ============================================================
# EXCEL PROCESSOR
# ============================================================

class ExcelProcessor:

    def __init__(self):
        self.party_cache: Dict[str, Tuple[str, float]] = {}

        self.date_formats = [
            '%d-%b-%Y', '%d-%b-%y', '%d/%m/%Y',
            '%d-%m-%Y', '%Y-%m-%d'
        ]

        self.business_suffixes = [
            'TRADERS', 'AGENCIES', 'ENTERPRISES', 'SERVICES', 'SOLUTIONS',
            'PVT', 'LTD', 'LIMITED', 'CORP', 'COMPANY', 'CO', 'GROUP',
            'BANK', 'PAYMENTS', 'FINTECH'
        ]

        self.known_merchants = {
            r'\bAMAZON\b': 'AMAZON',
            r'\bFLIPKART\b': 'FLIPKART',
            r'\bSWIGGY\b': 'SWIGGY',
            r'\bZOMATO\b': 'ZOMATO',
            r'\bPAYTM\b': 'PAYTM',
            r'\bPHONEPE\b': 'PHONEPE',
            r'\bGPAY\b': 'GOOGLE PAY',
            r'\bBHIM\b': 'BHIM',
            r'\bUBER\b': 'UBER',
            r'\bOLA\b': 'OLA',
            r'\bNETFLIX\b': 'NETFLIX',
            r'\bSPOTIFY\b': 'SPOTIFY',
            r'\bIRCTC\b': 'IRCTC',
        }

    # ========================================================
    # PUBLIC ENTRY
    # ========================================================

    def extract_transactions(self, file_content: bytes, filename: str = "") -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        transactions: List[Dict[str, Any]] = []
        account_profile: Dict[str, Any] = {}

        try:
            excel = pd.ExcelFile(io.BytesIO(file_content))

            for sheet_name in excel.sheet_names:
                try:
                    raw_df = pd.read_excel(excel, sheet_name=sheet_name, header=None)
                    account_profile.update(self._extract_account_profile(raw_df))

                    header_row = self._detect_header_row(raw_df)
                    if header_row is None:
                        continue

                    df = pd.read_excel(excel, sheet_name=sheet_name, header=header_row)
                    df.columns = [normalize_text(c) for c in df.columns]
                    df = self._normalize_columns(df)

                    sheet_txns = self._extract_from_dataframe(df, filename, sheet_name)
                    transactions.extend(sheet_txns)

                except Exception as e:
                    logger.warning(f"Sheet '{sheet_name}' skipped: {e}")

        except Exception as e:
            logger.error(f"Excel read error: {e}")

        return transactions, account_profile

    # ========================================================
    # ACCOUNT PROFILE
    # ========================================================

    def _extract_account_profile(self, df: pd.DataFrame) -> Dict[str, Any]:
        profile = {}
        header_text = " ".join(
            normalize_text(v)
            for i in range(min(15, len(df)))
            for v in df.iloc[i].values
            if pd.notna(v)
        )

        name_match = re.search(r'(ACCOUNT HOLDER|NAME)\s*[:\-]?\s*([A-Z\s]{3,})', header_text)
        if name_match:
            profile['account_holder_name'] = name_match.group(2).strip()

        acc_match = re.search(r'ACCOUNT\s*(NO|NUMBER)?\s*[:\-]?\s*([A-Z0-9]{6,})', header_text)
        if acc_match:
            profile['account_number'] = acc_match.group(2)

        return profile

    # ========================================================
    # HEADER & COLUMNS
    # ========================================================

    def _detect_header_row(self, df: pd.DataFrame):
        keywords = ['DATE', 'DESCRIPTION', 'DEBIT', 'CREDIT', 'BALANCE', 'AMOUNT']
        for idx, row in df.iterrows():
            row_text = " ".join(normalize_text(v) for v in row.values if pd.notna(v))
            if sum(1 for k in keywords if k in row_text) >= 2:
                return idx
        return None

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        col_map = {}

        for col in df.columns:
            c = col.upper()
            if 'DATE' in c:
                col_map[col] = 'date'
            elif any(x in c for x in ['DESC', 'NARRATION', 'PARTICULAR']):
                col_map[col] = 'description'
            elif 'CREDIT' in c or c == 'CR':
                col_map[col] = 'credit'
            elif 'DEBIT' in c or c == 'DR':
                col_map[col] = 'debit'
            elif 'BAL' in c:
                col_map[col] = 'balance'
            elif 'AMOUNT' in c:
                col_map[col] = 'amount'

        df = df.rename(columns=col_map)

        final_cols = [c for c in ['date', 'description', 'credit', 'debit', 'balance', 'amount'] if c in df.columns]
        return df[final_cols]

    # ========================================================
    # ROW PARSER
    # ========================================================

    def _extract_from_dataframe(self, df: pd.DataFrame, filename: str, sheet_name: str):
        transactions = []

        for _, row in df.iterrows():
            try:
                date = self._parse_date(row.get('date'))
                description = normalize_text(row.get('description'))

                credit = self._parse_amount(row.get('credit'))
                debit = self._parse_amount(row.get('debit'))
                balance = self._parse_amount(row.get('balance'))
                amount_col = self._parse_amount(row.get('amount'))

                if credit == debit == amount_col == 0 and not description:
                    continue

                if credit > 0:
                    amount = credit
                elif debit > 0:
                    amount = -debit
                else:
                    amount = amount_col

                party, confidence = self._extract_party(description)

                transactions.append({
                    'date': date,
                    'description': description,
                    'amount': amount,
                    'credit': credit,
                    'debit': debit,
                    'balance': balance,
                    'party': party,
                    'detected_party': party,
                    'party_confidence': round(confidence, 3),
                    'source': 'excel',
                    'source_file': filename,
                    'source_sheet': sheet_name,
                    'is_upi': bool(re.search(r'\b(UPI|@|GPAY|PHONEPE|PAYTM|BHIM)\b', description)),
                    'is_transfer': bool(re.search(r'\b(NEFT|IMPS|RTGS|TRANSFER|TRF)\b', description)),
                })

            except Exception as e:
                logger.debug(f"Row skipped: {e}")

        return transactions

    # ========================================================
    # DATE / AMOUNT
    # ========================================================

    def _parse_date(self, val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None

        if isinstance(val, datetime):
            return val.strftime('%d/%m/%Y')

        s = normalize_text(val)
        for fmt in self.date_formats:
            try:
                return datetime.strptime(s, fmt).strftime('%d/%m/%Y')
            except:
                continue

        return s

    def _parse_amount(self, val) -> float:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return 0.0
        s = re.sub(r'[^\d.-]', '', str(val))
        return safe_float(s)

    # ========================================================
    # PARTY EXTRACTION
    # ========================================================

    def _extract_party(self, narration: str) -> Tuple[str, float]:
        if not narration:
            return None, 0.0

        cache_key = narration[:80]
        if cache_key in self.party_cache:
            return self.party_cache[cache_key]

        # Known merchants
        for pat, name in self.known_merchants.items():
            if re.search(pat, narration):
                self.party_cache[cache_key] = (name, 0.95)
                return name, 0.95

        # UPI handles
        m = re.search(r'@([A-Z0-9]+)', narration)
        if m:
            party = m.group(1)
            self.party_cache[cache_key] = (party, 0.85)
            return party, 0.85

        # TO / FROM patterns
        m = re.search(r'\b(TO|FROM|BY)\s+([A-Z][A-Z\s]{2,})', narration)
        if m:
            party = self._clean_party(m.group(2))
            self.party_cache[cache_key] = (party, 0.7)
            return party, 0.7

        # Fallback meaningful words
        words = [w for w in narration.split() if len(w) > 3 and not w.isdigit()]
        party = self._clean_party(" ".join(words[:3])) if words else None
        confidence = 0.4 if party else 0.1

        self.party_cache[cache_key] = (party, confidence)
        return party, confidence

    def _clean_party(self, name: str):
        if not name:
            return None
        for s in self.business_suffixes:
            name = re.sub(rf'\b{s}\b', '', name)
        name = re.sub(r'[^\w\s]', ' ', name)
        return " ".join(name.split()).strip()

    # ========================================================
    # CACHE
    # ========================================================

    def clear_cache(self):
        self.party_cache.clear()
