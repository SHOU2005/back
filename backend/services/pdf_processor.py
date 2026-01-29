"""
PDF Processing Service - Extracts transactions from bank statement PDFs
Specialized parser for Bandhan Bank format
"""

import pdfplumber
import PyPDF2
import re
from typing import List, Dict, Any
from io import BytesIO
import logging

logger = logging.getLogger(__name__)


class PDFProcessor:
    """Process PDF bank statements and extract transaction data"""
    
    def __init__(self):
        self.month_map = {
            'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
            'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
            'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
        }
        self.amount_pattern = r'[\d,]+\.\d{2}'
    
    def extract_transactions(self, pdf_bytes: bytes) -> List[Dict[str, Any]]:
        """Extract all transactions from PDF bank statement."""
        transactions = []
        
        try:
            # Try pdfplumber first
            transactions = self._extract_with_pdfplumber(pdf_bytes)
            
            # Fallback to PyPDF2
            if not transactions or len(transactions) == 0:
                logger.info("Trying PyPDF2 extraction...")
                transactions = self._extract_with_pypdf2(pdf_bytes)
            
            if not transactions:
                raise ValueError("No valid transactions extracted from PDF")
            
            logger.info(f"Successfully extracted {len(transactions)} transactions")
            return transactions
            
        except Exception as e:
            logger.error(f"Error extracting transactions: {str(e)}")
            raise
    
    def _extract_with_pdfplumber(self, pdf_bytes: bytes) -> List[Dict[str, Any]]:
        """Extract using pdfplumber"""
        transactions = []
        try:
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        page_txns = self._parse_text(text)
                        transactions.extend(page_txns)
        except Exception as e:
            logger.warning(f"pdfplumber extraction error: {str(e)}")
        return transactions
    
    def _extract_with_pypdf2(self, pdf_bytes: bytes) -> List[Dict[str, Any]]:
        """Extract using PyPDF2"""
        transactions = []
        try:
            pdf_file = BytesIO(pdf_bytes)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            logger.info(f"PyPDF2: Processing {len(pdf_reader.pages)} pages")
            
            for page_num, page in enumerate(pdf_reader.pages):
                text = page.extract_text()
                if text:
                    logger.info(f"Page {page_num + 1}: {len(text)} chars")
                    page_txns = self._parse_text(text)
                    transactions.extend(page_txns)
        except Exception as e:
            logger.warning(f"PyPDF2 extraction error: {str(e)}")
        return transactions
    
    def _parse_text(self, text: str) -> List[Dict[str, Any]]:
        """Parse Bandhan Bank statement text"""
        transactions = []
        lines = text.split('\n')
        
        logger.info(f"Total lines: {len(lines)}")
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Skip header/empty lines
            if not line or self._is_header_line(line):
                i += 1
                continue
            
            # Look for transaction line with DD-MMM- pattern
            date_match = re.search(r'(\d{2})-([A-Za-z]{3})-', line)
            
            if date_match:
                day = date_match.group(1)
                month_str = date_match.group(2)
                
                # Get year from NEXT line if it looks like a year line
                year = None
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    year_match = re.match(r'^(\d{4})\s+\d{4}', next_line)
                    if year_match:
                        year = year_match.group(1)
                    elif re.match(r'^\d{4}\s+[A-Z]', next_line):
                        year_match = re.match(r'^(\d{4})', next_line)
                        if year_match:
                            year = year_match.group(1)
                
                if not year:
                    i += 1
                    continue
                
                # Build full date
                month = self.month_map.get(month_str.upper(), '01')
                date_str = f"{day.zfill(2)}/{month}/{year}"
                
                # Extract amounts from current line
                amounts = re.findall(self.amount_pattern, line)
                
                if amounts:
                    parsed_amounts = []
                    for a in amounts:
                        try:
                            val = float(a.replace(',', ''))
                            parsed_amounts.append(val)
                        except:
                            pass
                    
                    if len(parsed_amounts) >= 3:
                        # Format: debit, credit, balance
                        debit = parsed_amounts[0]
                        credit = parsed_amounts[1]
                        balance = parsed_amounts[2]
                        
                        # Skip if balance is unreasonable
                        if balance <= 100000000:
                            # Extract party name from description
                            party = self._extract_party_from_line(line)
                            
                            # Clean description for display
                            desc = self._clean_description(line)
                            
                            if party and len(party) >= 2:
                                txn = {
                                    'date': date_str,
                                    'description': desc,
                                    'credit': credit,
                                    'debit': debit,
                                    'balance': balance,
                                    'detected_party': party,
                                    'party': party
                                }
                                transactions.append(txn)
                                logger.info(f"Extracted: {date_str} | Party: {party} | ₹{credit+debit}")
                
                i += 2
            else:
                i += 1
        
        logger.info(f"Found {len(transactions)} transactions")
        return transactions
    
    def _extract_party_from_line(self, line: str) -> str:
        """Extract party name from transaction line"""
        desc = line.upper()
        
        # Pattern 1: UPI/CR/REF/NAME or UPI/DR/REF/NAME
        # Format: DEPOSIT, UPI/CR/204394168149/AMAN ASHOK VISHWAKARMA
        upi_match = re.search(r'UPI/(?:CR|DR)/\d+/([A-Z][A-Z\s]+?)(?:\s|$)', desc)
        if upi_match:
            party = upi_match.group(1).strip()
            party = self._clean_party_name(party)
            if party and len(party) >= 2:
                return party
        
        # Pattern 2: UPI/CR/REF/NAME (without / before name)
        upi_match = re.search(r'UPI/(?:CR|DR)/([A-Z][A-Z\s]+?)(?:\s|$)', desc)
        if upi_match:
            party = upi_match.group(1).strip()
            party = self._clean_party_name(party)
            if party and len(party) >= 2:
                return party
        
        # Pattern 3: ATM WDL location
        atm_match = re.search(r'WDL[,\s]+(?:AT\s+)?\+?\s*([A-Z][A-Z\s]+?)(?:MUMBAI|IN|KANDIVALI|$)', desc)
        if atm_match:
            party = atm_match.group(1).strip()
            party = self._clean_party_name(party)
            if party and len(party) >= 2:
                return party
        
        # Pattern 4: Generic party after DEPOSIT, CASH, etc.
        generic_match = re.search(r'(?:DEPOSIT|CASH|PAYMENT|TRANSFER)[,\s]+([A-Z][A-Z\s]+?)(?:\s*$|\d)', desc)
        if generic_match:
            party = generic_match.group(1).strip()
            party = self._clean_party_name(party)
            if party and len(party) >= 2:
                return party
        
        # Pattern 5: Extract last meaningful words
        words = desc.split()
        meaningful = []
        skip_words = {'DEPOSIT', 'CASH', 'UPI', 'CR', 'DR', 'WDL', 'ATM', 'DEP', 'SB', 
                      'CHARGES', 'FEE', 'GST', 'CARD', 'AMC', 'INTEREST', 'CASA', 
                      'CAPITALIZED', 'NACH', 'TRANSFER', 'PAYMENT', 'REVERSAL'}
        
        for w in words:
            w_clean = w.strip('.,-/')
            if (len(w_clean) > 2 and 
                w_clean.upper() not in skip_words and 
                not w_clean.isdigit() and
                not re.match(r'^\d+\.\d+$', w_clean)):
                meaningful.append(w_clean)
        
        if meaningful:
            return ' '.join(meaningful[:4])
        
        return "UNKNOWN"
    
    def _clean_description(self, line: str) -> str:
        """Clean transaction description for display"""
        desc = line
        
        # Remove date patterns
        desc = re.sub(r'\d{2}-[A-Za-z]{3}-', '', desc)
        desc = re.sub(r'\d{2}-[A-Za-z]{3}-', '', desc)
        
        # Remove amounts
        amounts = re.findall(self.amount_pattern, desc)
        for a in amounts:
            desc = desc.replace(a, '')
        
        # Clean up
        desc = re.sub(r'\s+', ' ', desc).strip()
        desc = re.sub(r'^[,\s\-]+|[,\s\-]+$', '', desc)
        
        return desc.strip()
    
    def _clean_party_name(self, name: str) -> str:
        """Clean party name"""
        if not name:
            return ""
        
        name = str(name).upper().strip()
        
        # Remove digits
        name = re.sub(r'\d+', '', name)
        
        # Remove special characters
        name = re.sub(r'[^\w\s]', ' ', name)
        
        # Remove common non-party words
        skip_words = {'DEPOSIT', 'CASH', 'UPI', 'CR', 'DR', 'WDL', 'ATM', 'DEP', 
                      'CHARGES', 'FEE', 'GST', 'CARD', 'AMC', 'SELF', 'IN', 'MUMBAI',
                      'KANDIVALI', 'GOREGAON', 'INDIA', 'TRANSFER', 'PAYMENT'}
        for word in skip_words:
            name = re.sub(r'\b' + word + r'\b', '', name, flags=re.IGNORECASE)
        
        # Clean up
        name = ' '.join(name.split()).strip()
        
        return name
    
    def _is_header_line(self, line: str) -> bool:
        """Check if line is a header/footer line to skip"""
        skip_patterns = [
            'Eachdepositor', 'Account,', 'Important',
            'Bandhan', 'Unless', 'Account No',
            'Product Type', 'Account Type', 'MAB',
            'Nominee', 'Joint Holder', 'Branch Address',
            'Opening Balance', 'Total Debit', 'Total Credit',
            'END OF STATEMENT', 'STATEMENT OF ACCOUNT',
            'DATEVALUE', 'DATECHEQUE', 'INSTRUMENT',
            'GOREGAON', 'From Date', 'To Date',
            'TRANS VALUE', 'CHEQUE /', 'DESCRIPTION',
            'DEBITS CREDITS', 'BALANCE', 'DATE DATE',
            'INSTRUMENT', 'Statement Summary',
            'Customer Number', 'Currency Name',
            'Capitalized'
        ]
        
        line_upper = line.upper()
        for pattern in skip_patterns:
            if pattern.upper() in line_upper:
                return True
        return False
