"""
PDF Processing Service - Extracts transactions from bank statement PDFs
Uses multiple parsing strategies for maximum accuracy
"""

import re
import logging
from io import BytesIO
from typing import List, Dict, Any

import pdfplumber
import PyPDF2
import pytesseract
from pdf2image import convert_from_bytes
from PIL import Image

logger = logging.getLogger(__name__)


class PDFProcessor:
    """Process PDF bank statements and extract transaction data"""

    def __init__(self):
        self.date_patterns = [
            r"\d{2}/\d{2}/\d{4}",
            r"\d{2}-\d{2}-\d{4}",
            r"\d{4}/\d{2}/\d{2}",
            r"\d{2}\.\d{2}\.\d{4}",
        ]
        self.amount_pattern = r"[\d,]+\.?\d{0,2}"

        self.upi_patterns = [
            r"UPI/(?:CR|DR)/\d+/(.+?)/(?:OK|FAIL|PA|BI|AX|PASS)",
            r"UPI-(?:CR|DR)?-?\d*-?(.+?)(?:[-/]OK|[-/]PA|[-/]BI|$)",
            r"@([a-zA-Z0-9]+)",
        ]

        self.transfer_patterns = [
            r"(RTGS|NEFT|IMPS)\s+(?:CR|DR)?\s*([A-Z][A-Za-z\s]{2,})",
            r"(?:PAID TO|RECEIVED FROM)\s+([A-Z][A-Za-z\s]{2,})",
        ]

        self.other_patterns = [
            r"SALARY\s+(?:FROM)?\s*([A-Z][A-Za-z\s]{2,})",
            r"CASH\s+(?:DEPOSIT|WITHDRAWAL)",
        ]

        self.merchant_patterns = {
            r"\bamazon\b": "AMAZON",
            r"\bflipkart\b": "FLIPKART",
            r"\bswiggy\b": "SWIGGY",
            r"\bzomato\b": "ZOMATO",
            r"\bpaytm\b": "PAYTM",
            r"\bphonepe\b": "PHONEPE",
            r"\bgpay\b": "GPAY",
        }

        self.business_suffixes = [
            "pvt", "ltd", "limited", "enterprise", "enterprises", "services"
        ]

    # ================= OCR EXTRACTION =================

    def _extract_with_ocr(self, pdf_bytes: bytes) -> List[Dict[str, Any]]:
        """Extract transactions using OCR for scanned PDFs"""
        transactions: List[Dict[str, Any]] = []

        try:
            images = convert_from_bytes(pdf_bytes, dpi=300)

            for idx, image in enumerate(images):
                text = pytesseract.image_to_string(image, lang="eng")

                if text and len(text.strip()) > 50:
                    logger.info(f"OCR page {idx + 1} text detected")
                    page_txns = self._parse_text_transactions(text)
                    transactions.extend(page_txns)

        except Exception as e:
            logger.error(f"OCR extraction failed: {e}")

        return transactions

    # ================= MAIN ENTRY =================

    def extract_transactions(self, pdf_bytes: bytes) -> List[Dict[str, Any]]:
        transactions = []

        # Strategy 1: pdfplumber
        transactions = self._extract_with_pdfplumber(pdf_bytes)

        # Strategy 2: PyPDF2
        if not transactions:
            logger.info("Trying PyPDF2 extraction...")
            transactions = self._extract_with_pypdf2(pdf_bytes)

        # Strategy 3: OCR
        if not transactions:
            logger.info("Trying OCR extraction...")
            transactions = self._extract_with_ocr(pdf_bytes)

        transactions = self._validate_and_clean(transactions)

        if not transactions:
            raise ValueError("No valid transactions extracted from PDF")

        logger.info(f"Extracted {len(transactions)} transactions")
        return transactions

    # ================= PDFPLUMBER =================

    def _extract_with_pdfplumber(self, pdf_bytes: bytes) -> List[Dict[str, Any]]:
        transactions = []

        try:
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        transactions.extend(self._parse_text_transactions(text))
        except Exception as e:
            logger.warning(f"pdfplumber error: {e}")

        return transactions

    # ================= PYPDF2 =================

    def _extract_with_pypdf2(self, pdf_bytes: bytes) -> List[Dict[str, Any]]:
        transactions = []

        try:
            reader = PyPDF2.PdfReader(BytesIO(pdf_bytes))
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    transactions.extend(self._parse_text_transactions(text))
        except Exception as e:
            logger.warning(f"PyPDF2 error: {e}")

        return transactions

    # ================= TEXT PARSING =================

    def _parse_text_transactions(self, text: str) -> List[Dict[str, Any]]:
        transactions = []
        lines = text.split("\n")

        for line in lines:
            date_match = re.search(self.date_patterns[0], line)
            if not date_match:
                continue

            amounts = re.findall(self.amount_pattern, line)
            amounts = [self._parse_amount(a) for a in amounts if self._parse_amount(a) > 0]

            txn = {
                "date": self._parse_date(date_match.group()),
                "description": line,
                "credit": 0.0,
                "debit": 0.0,
                "balance": amounts[-1] if amounts else 0.0,
            }

            if len(amounts) >= 2:
                txn["debit"] = amounts[0]

            txn["detected_party"] = self._extract_party_name(line)
            transactions.append(txn)

        return transactions

    # ================= HELPERS =================

    def _parse_date(self, date_str: str) -> str:
        parts = re.split(r"[\/\-\.]", date_str)
        if len(parts) == 3:
            return f"{parts[0].zfill(2)}/{parts[1].zfill(2)}/{parts[2]}"
        return None

    def _parse_amount(self, amount: str) -> float:
        try:
            return float(amount.replace(",", "").replace("₹", ""))
        except Exception:
            return 0.0

    def _extract_party_name(self, narration: str) -> str:
        if not narration:
            return "UNKNOWN"

        for pat, name in self.merchant_patterns.items():
            if re.search(pat, narration.lower()):
                return name

        for group in [self.upi_patterns, self.transfer_patterns, self.other_patterns]:
            for pat in group:
                m = re.search(pat, narration, re.IGNORECASE)
                if m:
                    return self._normalize_party_name(m.group(-1))

        return "UNKNOWN"

    def _normalize_party_name(self, name: str) -> str:
        name = name.upper()
        for suf in self.business_suffixes:
            name = re.sub(rf"\b{suf}\b", "", name)
        return " ".join(name.split())

    def _validate_and_clean(self, transactions: List[Dict]) -> List[Dict]:
        return [t for t in transactions if t.get("date") and t.get("debit", 0) + t.get("credit", 0) > 0]
