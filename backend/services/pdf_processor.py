"""
PDF Processing Service - Extracts transactions from bank statement PDFs
Uses multiple parsing strategies for maximum accuracy
"""

import pdfplumber
import PyPDF2
import re
from typing import List, Dict, Any
from io import BytesIO
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class PDFProcessor:
    """Process PDF bank statements and extract transaction data"""
    
    def __init__(self):
        self.date_patterns = [
            r'\d{2}/\d{2}/\d{4}',  # DD/MM/YYYY
            r'\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
            r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
            r'\d{2}\.\d{2}\.\d{4}', # DD.MM.YYYY
        ]
        self.amount_pattern = r'[\d,]+\.?\d{0,2}'
        
        # ========== COMPREHENSIVE UPI PATTERNS ==========
        self.upi_patterns = [
            r'UPI/(?:CR|DR)/\d+/(.+?)/(?:OK|FAIL|PA|BI|AX|PASS)',
            r'UPI/(?:CR|DR)/\d+/(.+?)$',
            r'UPI/\d+/(.+?)/(?:OK|FAIL|PA|BI)$',
            r'UPI/(.+?)/(?:OK|FAIL|PA|BI)$',
            r'UPI-(?:CR|DR)?-?\d*-?(.+?)(?:[-/]OK|[-/]FAIL|[-/]PA|[-/]BI|$)',
            r'UPI[-/]*(?:CR|DR)?[-/]*\d*[-/]*(.+?)(?:[-/]OK|[-/]PA|[-/]BI|$)',
            r'@([a-zA-Z0-9]+)',
            r'UPI[/\s]*@([a-zA-Z0-9]+)',
            r'UPI[/\s]*(?:from|to|by)[/\s]*([A-Z][A-Za-z\s]{2,})(?:\s*$|/)',
            r'UPI/(?:D\d+)?[/\s]*([A-Z][A-Za-z\s]{2,})(?:\s*$|/)',
            r'UPI[/\s]*(?:CR|DR)[/\s]*(?:D\d+)?[/\s]*([A-Z][A-Za-z\s]{2,})(?:\s*$)',
            r'(?:UPI|PAYTM|GPAY|PHONEPE)[/\s]*(?:CR|DR)?[/\s]*(?:D\d+)?[/\s]*([A-Z][A-Za-z\s]+?)(?:/OKPA|/OKAX|/OKBI|/OK|/PAYPASS|$)',
        ]
        
        # ========== RTGS/NEFT/IMPS PATTERNS ==========
        self.transfer_patterns = [
            r'RTGS\s+(?:CR|DR)?[-]?\s*(?:[A-Z0-9]+[-])?\s*([A-Z][A-Za-z\s]{2,})',
            r'NEFT\s+(?:CR|DR)?[-]?\s*(?:[A-Z0-9]+[-])?\s*([A-Z][A-Za-z\s]{2,})',
            r'IMPS\s+(?:CR|DR)?[-]?\s*(?:[A-Z0-9]+[-])?\s*([A-Z][A-Za-z\s]{2,})',
            r'(?:transfer|TRANSFER)\s+(?:from|to|FROM|TO)\s+([A-Z][A-Za-z\s]{2,})',
            r'PAID\s+TO\s+([A-Z][A-Za-z\s]{2,})',
            r'RECEIVED\s+FROM\s+([A-Z][A-Za-z\s]{2,})',
            r'BY\s+(?:TRANSFER|NEFT|RTGS|IMPS)[:\s-]*([A-Z][A-Za-z\s]{2,})',
            r'TRF\s+(?:TO|FROM)[:\s]*([A-Z][A-Za-z\s]{2,})',
        ]
        
        # ========== CASH/BILL PATTERNS ==========
        self.other_patterns = [
            r'CASH\s+(?:DEPOSIT|WITHDRAWAL)\s*(?:AT|BY)?\s*([A-Z][A-Za-z\s]{2,})',
            r'(?:BILL|EMI|LOAN)\s+(?:PAYMENT|REPAYMENT)[:\s]*([A-Z][A-Za-z\s]{2,})',
            r'INSURANCE\s+(?:PREMIUM|PAYMENT)[:\s]*([A-Z][A-Za-z\s]{2,})',
            r'SALARY\s+(?:FROM|TO)?\s*([A-Z][A-Za-z\s]{2,})',
        ]
        
        # ========== MERCHANT PATTERNS ==========
        self.merchant_patterns = {
            r'\buber\b': 'UBER', r'\bola\b': 'OLA', r'\bswiggy\b': 'SWIGGY',
            r'\bzomato\b': 'ZOMATO', r'\bamazon\b': 'AMAZON', r'\bflipkart\b': 'FLIPKART',
            r'\bmyntra\b': 'MYNTRA', r'\boyo\b': 'OYO', r'\birctc\b': 'IRCTC',
            r'\bmakemytrip\b': 'MAKE MY TRIP', r'\bredbus\b': 'REDBUS',
            r'\bpaytm\b': 'PAYTM', r'\bphonepe\b': 'PHONEPE', r'\bgpay\b': 'GPAY',
            r'\bbhim\b': 'BHIM', r'\bgoogle\b': 'GOOGLE', r'\bnetflix\b': 'NETFLIX',
            r'\bspotify\b': 'SPOTIFY', r'\bhotstar\b': 'HOTSTAR',
        }
        
        # ========== BUSINESS SUFFIXES ==========
        self.business_suffixes = [
            'traders', 'trdg', 'trd', 'agencies', 'agy', 'enterprises', 'entp',
            'services', 'srv', 'solutions', 'soln', 'pvt', 'ltd', 'limited',
            'corp', 'corporation', 'inc', 'company', 'co', 'group', 'associates',
        ]
    
    def extract_transactions(self, pdf_bytes: bytes) -> List[Dict[str, Any]]:
        """
        Extract all transactions from PDF bank statement.
        Uses multiple strategies to ensure accuracy and completeness.
        """
        transactions = []
        
        try:
            # Strategy 1: pdfplumber (best for structured tables)
            transactions = self._extract_with_pdfplumber(pdf_bytes)
            
            # Strategy 2: PyPDF2 text extraction (fallback)
            if not transactions or len(transactions) == 0:
                logger.info("Trying PyPDF2 extraction...")
                transactions = self._extract_with_pypdf2(pdf_bytes)
            
            # Validate and clean transactions
            transactions = self._validate_and_clean(transactions)
            
            # Extract party names for all transactions
            for txn in transactions:
                if not txn.get('detected_party'):
                    txn['detected_party'] = self._extract_party_name(txn.get('description', ''))
            
            if not transactions:
                raise ValueError("No valid transactions extracted from PDF")
            
            logger.info(f"Successfully extracted {len(transactions)} transactions")
            return transactions
            
        except Exception as e:
            logger.error(f"Error extracting transactions: {str(e)}")
            raise
    
    def _extract_party_name(self, narration):
        """
        Extract party name from transaction narration.
        """
        if not narration or len(narration) < 2:
            return None
        
        narration_clean = narration.strip()
        
        party = None
        
        # ========== STEP 1: Check known merchants ==========
        for pattern, name in self.merchant_patterns.items():
            if re.search(pattern, narration_clean.lower()):
                party = name
                logger.debug(f"Merchant match: '{narration}' -> '{party}'")
                break
        
        # ========== STEP 2: Try all pattern groups ==========
        if not party:
            pattern_groups = [
                self.upi_patterns,
                self.transfer_patterns,
                self.other_patterns,
            ]
            
            for group_patterns in pattern_groups:
                for pattern in group_patterns:
                    match = re.search(pattern, narration, re.IGNORECASE)
                    if match:
                        try:
                            if match.group(1):
                                candidate = match.group(1).upper().strip()
                                candidate = ' '.join(candidate.split())
                                if (len(candidate) >= 2 and 
                                    candidate not in ['DR', 'CR', 'TRF', 'BY', 'TO', 'FROM', 'PAID', 'RECEIVED']):
                                    party = self._normalize_party_name(candidate)
                                    logger.debug(f"Party match: '{narration}' -> '{party}'")
                                    break
                        except IndexError:
                            continue
                if party:
                    break
        
        # ========== STEP 3: Fallback extraction ==========
        if not party:
            party = self._extract_party_advanced(narration_clean)
        
        return party
    
    def _extract_party_advanced(self, narration):
        """
        Advanced party extraction for complex narrations.
        """
        # Try patterns like "TO PARTYNAME", "FROM PARTYNAME"
        patterns = [
            r'TO\s+([A-Z][A-Za-z\s]{2,})',
            r'FOR\s+([A-Z][A-Za-z\s]{2,})',
            r'FROM\s+([A-Z][A-Za-z\s]{2,})',
            r'AT\s+([A-Z][A-Za-z\s]{2,})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, narration, re.IGNORECASE)
            if match:
                candidate = match.group(1).upper().strip()
                candidate = ' '.join(candidate.split())
                candidate = re.sub(r'^(TO|FROM|FOR|AT|ON|BY|REF|REFNO|NO|NEW|AC|ACC)\s*', '', candidate)
                if len(candidate) >= 2:
                    return self._normalize_party_name(candidate)
        
        # Last resort: extract meaningful words
        transaction_words = ['DEPOSIT', 'WITHDRAWAL', 'PAYMENT', 'TRANSFER', 'CREDIT', 'DEBIT',
                           'BALANCE', 'CHARGES', 'FEE', 'TAX', 'EMI', 'BILL', 'SALARY',
                           'INTEREST', 'DIVIDEND', 'REFUND', 'REVERSAL', 'CLEARING']
        cleaned = narration
        for word in transaction_words:
            cleaned = re.sub(r'\b' + word + r'\b', ' ', cleaned, flags=re.IGNORECASE)
        
        words = cleaned.strip().split()
        meaningful = [w for w in words if len(w) > 2 and not w.isdigit()]
        
        if meaningful:
            party = ' '.join(meaningful[:3]).upper()
            return self._normalize_party_name(party)
        
        return None
    
    def _normalize_party_name(self, name):
        """
        Normalize party name by removing suffixes and cleaning up.
        """
        if not name:
            return ""
        
        name = str(name).upper().strip()
        
        # Remove business suffixes
        for suffix in self.business_suffixes:
            name = re.sub(rf'\b{suffix}\b', '', name, flags=re.IGNORECASE)
        
        # Remove long number sequences
        name = re.sub(r'\b[\d]{10,}\b', '', name)
        
        # Remove special characters
        name = re.sub(r'[^\w\s]', ' ', name)
        
        # Remove common prefixes
        name = re.sub(r'^(?:TO|FROM|FOR|VIA|BY|REF|REFNO|NO|NEW|AC|ACC|ACCOUNT|TRANSFER|TRF)\s*', '', name, flags=re.IGNORECASE)
        
        # Clean up whitespace
        name = ' '.join(name.split())
        
        result = name.strip()
        
        if len(result) >= 2:
            return result
        
        return "UNKNOWN"
    
    def _extract_with_pdfplumber(self, pdf_bytes: bytes) -> List[Dict[str, Any]]:
        """Extract transactions using pdfplumber (handles tables well)"""
        transactions = []
        
        try:
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    # Try to extract tables first
                    tables = page.extract_tables()
                    
                    for table in tables:
                        if not table or len(table) < 2:
                            continue
                        
                        # Find header row
                        header_row_idx = self._find_header_row(table)
                        if header_row_idx is None:
                            continue
                        
                        headers = [str(cell).strip().lower() if cell else '' for cell in table[header_row_idx]]
                        
                        # Process data rows
                        for row_idx in range(header_row_idx + 1, len(table)):
                            row = table[row_idx]
                            if not row or all(not cell for cell in row):
                                continue
                            
                            txn = self._parse_table_row(headers, row)
                            if txn:
                                transactions.append(txn)
                    
                    # If no tables found, extract text and parse
                    if not tables:
                        text = page.extract_text()
                        if text:
                            page_txns = self._parse_text_transactions(text)
                            transactions.extend(page_txns)
        
        except Exception as e:
            logger.warning(f"pdfplumber extraction error: {str(e)}")
        
        return transactions
    
    def _extract_with_pypdf2(self, pdf_bytes: bytes) -> List[Dict[str, Any]]:
        """Extract transactions using PyPDF2 (text-based parsing)"""
        transactions = []
        
        try:
            pdf_file = BytesIO(pdf_bytes)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            for page_num, page in enumerate(pdf_reader.pages):
                text = page.extract_text()
                if text:
                    page_txns = self._parse_text_transactions(text)
                    transactions.extend(page_txns)
        
        except Exception as e:
            logger.warning(f"PyPDF2 extraction error: {str(e)}")
        
        return transactions
    
    def _find_header_row(self, table: List[List]) -> int:
        """Find the header row in a table"""
        header_keywords = ['date', 'description', 'narration', 'credit', 'debit', 'balance', 'amount']
        
        for idx, row in enumerate(table[:5]):  # Check first 5 rows
            if not row:
                continue
            row_text = ' '.join([str(cell).lower() if cell else '' for cell in row])
            if any(keyword in row_text for keyword in header_keywords):
                return idx
        return 0
    
    def _parse_table_row(self, headers: List[str], row: List) -> Dict[str, Any]:
        """Parse a table row into transaction dictionary"""
        try:
            txn = {
                'date': None,
                'description': '',
                'credit': 0.0,
                'debit': 0.0,
                'balance': 0.0,
                'detected_party': None,
            }
            
            # Map headers to fields
            date_idx = self._find_column_index(headers, ['date', 'transaction date'])
            desc_idx = self._find_column_index(headers, ['description', 'narration', 'particulars', 'details'])
            credit_idx = self._find_column_index(headers, ['credit', 'cr', 'deposit'])
            debit_idx = self._find_column_index(headers, ['debit', 'dr', 'withdrawal'])
            balance_idx = self._find_column_index(headers, ['balance', 'bal'])
            
            # Extract values
            if date_idx is not None and date_idx < len(row):
                txn['date'] = self._parse_date(str(row[date_idx]) if row[date_idx] else '')
            
            if desc_idx is not None and desc_idx < len(row):
                txn['description'] = str(row[desc_idx]).strip() if row[desc_idx] else ''
            
            if credit_idx is not None and credit_idx < len(row):
                txn['credit'] = self._parse_amount(str(row[credit_idx]) if row[credit_idx] else '')
            
            if debit_idx is not None and debit_idx < len(row):
                txn['debit'] = self._parse_amount(str(row[debit_idx]) if row[debit_idx] else '')
            
            if balance_idx is not None and balance_idx < len(row):
                txn['balance'] = self._parse_amount(str(row[balance_idx]) if row[balance_idx] else '')
            
            # Extract party name
            if txn['description']:
                party = self._extract_party_name(txn['description'])
                txn['detected_party'] = party
                txn['party'] = party
            
            # Validate transaction has required fields
            if txn['date'] and txn['description']:
                return txn
        
        except Exception as e:
            logger.debug(f"Error parsing table row: {str(e)}")
        
        return None
    
    def _find_column_index(self, headers: List[str], keywords: List[str]) -> int:
        """Find column index by keywords"""
        for keyword in keywords:
            for idx, header in enumerate(headers):
                if keyword in header:
                    return idx
        return None
    
    def _parse_text_transactions(self, text: str) -> List[Dict[str, Any]]:
        """Parse transactions from unstructured text"""
        transactions = []
        lines = text.split('\n')
        
        current_txn = None
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Try to find date pattern
            date_match = re.search(self.date_patterns[0], line)
            if date_match:
                # Save previous transaction
                if current_txn and current_txn.get('description'):
                    transactions.append(current_txn)
                
                # Start new transaction
                date_str = date_match.group()
                current_txn = {
                    'date': self._parse_date(date_str),
                    'description': '',
                    'credit': 0.0,
                    'debit': 0.0,
                    'balance': 0.0,
                    'detected_party': None,
                }
                
                # Extract amounts from line
                amounts = re.findall(self.amount_pattern, line)
                amounts = [self._parse_amount(a) for a in amounts if self._parse_amount(a) > 0]
                
                if len(amounts) >= 1:
                    if len(amounts) >= 2:
                        current_txn['balance'] = amounts[-1]
                        if amounts[0] != amounts[-1]:
                            if amounts[0] > amounts[-1]:
                                current_txn['debit'] = amounts[0] - amounts[-1]
                            else:
                                current_txn['credit'] = amounts[-1] - amounts[0]
                    else:
                        current_txn['balance'] = amounts[0]
                
                # Extract description (text between date and amounts)
                desc_part = line[:date_match.start()] + line[date_match.end():]
                desc_part = re.sub(self.amount_pattern, '', desc_part).strip()
                if desc_part:
                    current_txn['description'] = desc_part
                    party = self._extract_party_name(desc_part)
                    current_txn['detected_party'] = party
                    current_txn['party'] = party
            elif current_txn:
                # Continue building description
                amounts = re.findall(self.amount_pattern, line)
                if amounts:
                    # Line contains amounts, might be credit/debit/balance
                    amounts = [self._parse_amount(a) for a in amounts if self._parse_amount(a) > 0]
                    if amounts:
                        if not current_txn['balance']:
                            current_txn['balance'] = amounts[-1]
                        elif not current_txn['credit'] and not current_txn['debit']:
                            if amounts[0] > current_txn['balance']:
                                current_txn['credit'] = amounts[0] - current_txn['balance']
                            else:
                                current_txn['debit'] = current_txn['balance'] - amounts[0]
                else:
                    # Continue description
                    if current_txn['description']:
                        current_txn['description'] += ' ' + line
                    else:
                        current_txn['description'] = line
                        party = self._extract_party_name(line)
                        current_txn['detected_party'] = party
                        current_txn['party'] = party
        
        # Add last transaction
        if current_txn and current_txn.get('description'):
            transactions.append(current_txn)
        
        return transactions
    
    def _parse_date(self, date_str: str) -> str:
        """Parse date string to DD/MM/YYYY format"""
        if not date_str:
            return None
        
        date_str = date_str.strip()
        
        # Try different date patterns
        for pattern in self.date_patterns:
            match = re.search(pattern, date_str)
            if match:
                date_str = match.group()
                # Normalize to DD/MM/YYYY
                parts = re.split(r'[/\-\.]', date_str)
                if len(parts) == 3:
                    day, month, year = parts
                    # Handle YYYY/MM/DD format
                    if len(year) == 4 and int(year) > 31:
                        year, month, day = parts
                    return f"{day.zfill(2)}/{month.zfill(2)}/{year}"
        
        return None
    
    def _parse_amount(self, amount_str: str) -> float:
        """Parse amount string to float with improved recovery"""
        if not amount_str:
            return 0.0
        
        try:
            # Remove commas and other formatting
            amount_str = str(amount_str).replace(',', '').replace('₹', '').replace('$', '').replace('Rs.', '').replace('rs.', '').strip()
            
            # Handle negative amounts (parentheses or minus sign)
            is_negative = False
            if amount_str.startswith('(') and amount_str.endswith(')'):
                amount_str = amount_str[1:-1]
                is_negative = True
            elif amount_str.startswith('-'):
                amount_str = amount_str[1:]
                is_negative = True
            
            # Extract numeric value with decimal support
            match = re.search(r'\d+\.?\d{0,2}', amount_str)
            if match:
                value = float(match.group())
                return -value if is_negative else value
        except (ValueError, AttributeError, TypeError):
            pass
        
        return 0.0
    
    def _validate_and_clean(self, transactions: List[Dict]) -> List[Dict]:
        """Validate and clean extracted transactions with recovery logic"""
        validated = []
        seen = set()
        balances = []
        
        # First pass: collect all balances for continuity validation
        for txn in transactions:
            balance = float(txn.get('balance', 0))
            if balance > 0:
                balances.append(balance)
        
        # Calculate median balance change for recovery
        balance_changes = []
        prev_balance = None
        for txn in transactions:
            balance = float(txn.get('balance', 0))
            if balance > 0 and prev_balance is not None:
                balance_changes.append(abs(balance - prev_balance))
            if balance > 0:
                prev_balance = balance
        
        median_change = sorted(balance_changes)[len(balance_changes) // 2] if balance_changes else 0
        
        # Second pass: validate and recover transactions
        prev_balance = None
        for idx, txn in enumerate(transactions):
            # Skip duplicates
            txn_key = (txn.get('date'), txn.get('description')[:50], round(txn.get('credit', 0), 2), round(txn.get('debit', 0), 2))
            if txn_key in seen:
                continue
            seen.add(txn_key)
            
            # Validate required fields
            if not txn.get('date') or not txn.get('description'):
                # Try to recover date from context
                if idx > 0 and not txn.get('date'):
                    prev_txn = transactions[idx - 1]
                    if prev_txn.get('date'):
                        txn['date'] = prev_txn.get('date')
                
                # Skip if still no date or description
                if not txn.get('date') or not txn.get('description'):
                    continue
            
            credit = float(txn.get('credit', 0))
            debit = float(txn.get('debit', 0))
            balance = float(txn.get('balance', 0))
            
            # Recovery logic: if both credit and debit are 0, try to infer from balance
            if credit == 0 and debit == 0:
                # Use balance continuity to recover amount
                if prev_balance is not None and balance > 0:
                    balance_diff = balance - prev_balance
                    if balance_diff > 0:
                        credit = abs(balance_diff)
                    else:
                        debit = abs(balance_diff)
                    txn['credit'] = credit
                    txn['debit'] = debit
                elif balance > 0 and median_change > 0:
                    # Use median change as estimate (conservative)
                    txn['debit'] = median_change  # Default to debit for unknown
                    debit = median_change
                else:
                    # Last resort: try regex extraction from description
                    amount_str = re.search(r'[\d,]+\.?\d{0,2}', txn.get('description', ''))
                    if amount_str:
                        recovered_amount = self._parse_amount(amount_str.group())
                        if recovered_amount > 0:
                            txn['debit'] = recovered_amount
                            debit = recovered_amount
            
            # Validate: ensure at least one amount is non-zero
            if txn.get('credit', 0) == 0 and txn.get('debit', 0) == 0:
                # Still zero? Use regex fallback on description
                desc = str(txn.get('description', ''))
                amounts = re.findall(self.amount_pattern, desc)
                amounts = [self._parse_amount(a) for a in amounts if self._parse_amount(a) > 0]
                if amounts:
                    # Use largest amount found in description
                    recovered_amount = max(amounts)
                    if recovered_amount > 100:  # Reasonable threshold
                        txn['debit'] = recovered_amount
                        debit = recovered_amount
            
            # Balance continuity validation and correction
            if prev_balance is not None and balance > 0:
                expected_balance = prev_balance + credit - debit
                balance_diff = abs(expected_balance - balance)
                
                # If balance doesn't match, try to correct credit/debit
                if balance_diff > 1.0 and credit == 0 and debit == 0:
                    # Recover from balance change
                    actual_change = balance - prev_balance
                    if actual_change > 0:
                        txn['credit'] = actual_change
                        credit = actual_change
                    else:
                        txn['debit'] = abs(actual_change)
                        debit = abs(actual_change)
            
            # Final validation: skip if still no valid amounts (very rare)
            if txn.get('credit', 0) == 0 and txn.get('debit', 0) == 0:
                logger.warning(f"Skipping transaction with no amounts: {txn.get('description', 'N/A')[:50]}")
                continue
            
            # Format date
            if txn['date']:
                txn['date'] = str(txn['date'])
            
            # Update previous balance
            if balance > 0:
                prev_balance = balance
            elif credit > 0:
                prev_balance = (prev_balance or 0) + credit
            elif debit > 0:
                prev_balance = (prev_balance or 0) - debit
            
            validated.append(txn)
        
        logger.info(f"Validated {len(validated)} transactions (recovered {len(validated) - len(transactions) + len([t for t in transactions if t.get('date') and t.get('description')])} missing amounts)")
        return validated

