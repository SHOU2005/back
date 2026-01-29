"""
Universal Bank Statement PDF Analyzer
- Supports ANY bank (SBI, HDFC, ICICI, Axis, Bandhan, etc.)
- Handles table & non-table PDFs
- Multi-line transactions
- Debit / Credit auto detection
- FastAPI ready
"""

import re
import logging
from typing import List, Dict, Any
from io import BytesIO

import pdfplumber
import PyPDF2
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse

# -------------------------------------------------
# LOGGING
# -------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pdf_processor")

# -------------------------------------------------
# PDF PROCESSOR
# -------------------------------------------------

class PDFProcessor:

    DATE_REGEX = re.compile(
        r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{1,2}-[A-Za-z]{3}-\d{2,4})\b'
    )

    AMOUNT_REGEX = re.compile(r'[\d,]+\.\d{2}')

    SKIP_WORDS = {
        "UPI", "IMPS", "NEFT", "RTGS", "DR", "CR", "DEBIT", "CREDIT",
        "TRANSFER", "PAYMENT", "WITHDRAWAL", "ATM", "WDL",
        "BANK", "INDIA", "ONLINE", "MOBILE"
    }

    # ===============================
    # PUBLIC METHOD
    # ===============================

    def extract_transactions(self, pdf_bytes: bytes) -> List[Dict[str, Any]]:
        text = self._extract_text(pdf_bytes)

        if not text.strip():
            return []

        return self._parse_transactions(text)

    # ===============================
    # TEXT EXTRACTION
    # ===============================

    def _extract_text(self, pdf_bytes: bytes) -> str:
        text = ""

        # ---- pdfplumber (best)
        try:
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        text += t + "\n"
        except Exception as e:
            logger.warning(f"pdfplumber failed: {e}")

        if text.strip():
            return text

        # ---- PyPDF2 fallback
        try:
            reader = PyPDF2.PdfReader(BytesIO(pdf_bytes))
            for page in reader.pages:
                t = page.extract_text()
                if t:
                    text += t + "\n"
        except Exception as e:
            logger.warning(f"PyPDF2 failed: {e}")

        return text

    # ===============================
    # TRANSACTION PARSER
    # ===============================

    def _parse_transactions(self, text: str) -> List[Dict[str, Any]]:
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        transactions = []

        i = 0
        while i < len(lines):
            line = lines[i]

            date_match = self.DATE_REGEX.search(line)
            if not date_match:
                i += 1
                continue

            date = self._normalize_date(date_match.group(1))

            block = [line]
            j = i + 1
            while j < len(lines) and not self.DATE_REGEX.search(lines[j]):
                block.append(lines[j])
                j += 1

            txn = self._parse_block(date, block)
            if txn:
                transactions.append(txn)

            i = j

        return transactions

    # ===============================
    # BLOCK LOGIC
    # ===============================

    def _parse_block(self, date: str, block: List[str]) -> Dict[str, Any]:
        text = " ".join(block)

        amounts = [float(a.replace(",", "")) for a in self.AMOUNT_REGEX.findall(text)]

        debit = credit = balance = None

        if len(amounts) == 1:
            debit = amounts[0]
        elif len(amounts) == 2:
            debit, balance = amounts
        elif len(amounts) >= 3:
            debit, credit, balance = amounts[-3:]

        txn_type = self._detect_type(text, debit, credit)
        party = self._extract_party(text)
        description = self._clean_description(text)

        return {
            "date": date,
            "description": description,
            "party": party,
            "type": txn_type,
            "debit": debit if txn_type == "DEBIT" else 0.0,
            "credit": credit if txn_type == "CREDIT" else 0.0,
            "balance": balance
        }

    # ===============================
    # HELPERS
    # ===============================

    def _detect_type(self, text: str, debit, credit) -> str:
        t = text.upper()

        if any(x in t for x in [" CREDIT", " CR", " DEPOSIT"]):
            return "CREDIT"
        if any(x in t for x in [" DEBIT", " DR", " WDL", " WITHDRAWAL"]):
            return "DEBIT"

        if credit and not debit:
            return "CREDIT"
        return "DEBIT"

    def _extract_party(self, text: str) -> str:
        text = text.upper()

        patterns = [
            r'UPI/(?:CR|DR)/\d+/([A-Z\s]+)',
            r'IMPS/\d+/([A-Z\s]+)',
            r'NEFT/([A-Z\s]+)',
            r'TRANSFER TO ([A-Z\s]+)',
            r'FROM ([A-Z\s]+)'
        ]

        for p in patterns:
            m = re.search(p, text)
            if m:
                return self._clean_party(m.group(1))

        words = [
            w for w in text.split()
            if w.isalpha() and len(w) > 3 and w not in self.SKIP_WORDS
        ]

        return " ".join(words[:4]) if words else "UNKNOWN"

    def _clean_party(self, name: str) -> str:
        name = re.sub(r'[^A-Z\s]', '', name)
        return " ".join(name.split()).strip()

    def _clean_description(self, text: str) -> str:
        text = self.DATE_REGEX.sub("", text)
        text = self.AMOUNT_REGEX.sub("", text)
        return " ".join(text.split()).strip()

    def _normalize_date(self, raw: str) -> str:
        raw = raw.replace("-", "/")
        d, m, y = raw.split("/")

        if len(y) == 2:
            y = "20" + y

        return f"{d.zfill(2)}/{m.zfill(2)}/{y}"

# -------------------------------------------------
# FASTAPI APP
# -------------------------------------------------

app = FastAPI(title="Universal Bank PDF Analyzer")

processor = PDFProcessor()

@app.post("/analyze")
async def analyze_pdf(file: UploadFile = File(...)):
    pdf_bytes = await file.read()

    transactions = processor.extract_transactions(pdf_bytes)

    return JSONResponse({
        "status": "success",
        "total_transactions": len(transactions),
        "transactions": transactions
    })
