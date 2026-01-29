"""
Fund Flow Chain Builder - Trace Money Movement Between Parties
Builds complete fund flow paths: Party A -> Party B -> Party C -> ...
"""

from typing import List, Dict, Optional, Set, Any
from dataclasses import dataclass, field
from collections import defaultdict
import logging
from datetime import datetime
import re

logger = logging.getLogger(__name__)


@dataclass
class Transaction:
    """Represents a single transaction."""
    date: str
    description: str
    amount: float
    credit: float
    debit: float
    category: str
    is_upi: bool
    is_transfer: bool
    party: Optional[str] = None
    source_file: Optional[str] = None
    narration: Optional[str] = None
    
    def __hash__(self):
        return hash((self.date, self.description, self.amount))
    
    def __eq__(self, other):
        if not isinstance(other, Transaction):
            return False
        return (self.date == other.date and 
                self.description == other.description and 
                self.amount == other.amount)


@dataclass
class FundFlowChain:
    """Represents a chain of money flow between parties."""
    chain_id: str
    flow_path: str
    total_amount: float
    confidence: float
    chain_depth: int
    cross_pdf_links: int
    transactions: List[Transaction] = field(default_factory=list)
    flow_path_list: List[str] = field(default_factory=list)


class FundFlowChainBuilder:
    """
    Builds fund flow chains by correlating transactions across parties.
    """
    
    def __init__(self):
        self.transactions: List[Transaction] = []
        self.chains: List[FundFlowChain] = []
        self.party_transactions: Dict[str, List[Transaction]] = defaultdict(list)
        self.source_files: Set[str] = set()
        
        # Correlation thresholds
        self.AMOUNT_TOLERANCE = 2.0
        self.DATE_TOLERANCE_DAYS = 1
        self.MIN_CONFIDENCE = 0.5
    
    def add_transactions(self, transactions: List[Dict], source_file: str = ""):
        """Add transactions to the analyzer."""
        self.source_files.add(source_file)
        
        for txn_data in transactions:
            try:
                txn = Transaction(
                    date=str(txn_data.get('date', '')),
                    description=str(txn_data.get('description', '')),
                    amount=float(txn_data.get('amount', 0) or 0),
                    credit=float(txn_data.get('credit', 0) or 0),
                    debit=float(txn_data.get('debit', 0) or 0),
                    category=str(txn_data.get('category', 'unknown')),
                    is_upi=bool(txn_data.get('is_upi', False)),
                    is_transfer=bool(txn_data.get('is_transfer', False) or txn_data.get('is_upi', False)),
                    party=txn_data.get('detected_party') or txn_data.get('party'),
                    source_file=source_file,
                    narration=txn_data.get('description', '')
                )
                
                self.transactions.append(txn)
                
                party = txn.party or self._extract_party_from_narration(txn.description)
                if party:
                    self.party_transactions[party.upper()].append(txn)
                    
            except Exception as e:
                logger.warning(f"Error creating transaction: {e}")
                continue
    
    def _extract_party_from_narration(self, narration: str) -> Optional[str]:
        """Extract party name from narration."""
        if not narration:
            return None
        
        narration = str(narration).upper()
        
        patterns = [
            r'UPI[/\s]*(?:CR|DR)[/\s]*[\d]+[/\s]*([A-Z\s]+)',
            r'PAID\s+TO\s+([A-Z\s]+)',
            r'TRANSFER\s+TO\s+([A-Z\s]+)',
            r'(?:NEFT|IMPS)\s+(?:DR|CR)?\s*([A-Z\s]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, narration)
            if match:
                party = match.group(1).strip()
                party = re.sub(r'[0-9#*]+', '', party)
                party = ' '.join(party.split())
                if len(party) >= 2:
                    return party
        
        return None
    
    def _normalize_party_name(self, name: str) -> str:
        """Normalize party name for comparison."""
        if not name:
            return ""
        
        name = str(name).upper().strip()
        
        suffixes = [
            'TRADERS', 'TRDG', 'TRD', 'AGENCIES', 'AGY', 'ENTERPRISES', 'ENTP',
            'SERVICES', 'SRV', 'SOLUTIONS', 'SOLN', 'PVT', 'LTD', 'LIMITED',
            'CORP', 'CORPORATION', 'INC', 'COMPANY', 'CO'
        ]
        
        for suffix in suffixes:
            name = re.sub(r'\b' + suffix + r'\b', '', name, flags=re.IGNORECASE).strip()
        
        name = ' '.join(name.split())
        return name
    
    def build_chains(self):
        """Build fund flow chains from transactions."""
        self.chains = []
        
        if not self.transactions:
            return
        
        credits = [t for t in self.transactions if t.credit > 0]
        debits = [t for t in self.transactions if t.debit > 0]
        
        credit_by_amount = self._group_by_amount(credits)
        debit_by_amount = self._group_by_amount(debits)
        
        chain_id = 0
        visited_pairs = set()
        
        for debit in debits:
            debit_party = debit.party or "Unknown"
            
            matching_credits = self._find_matching_credits(
                debit.amount, debit.date, debit_party, debit, credit_by_amount, visited_pairs
            )
            
            if matching_credits:
                for credit in matching_credits:
                    chain = self._build_single_chain(
                        debit, credit, chain_id, visited_pairs
                    )
                    if chain:
                        self.chains.append(chain)
                        chain_id += 1
        
        for credit in credits:
            credit_party = credit.party or "Unknown"
            
            matching_debits = self._find_matching_debits(
                credit.amount, credit.date, credit_party, credit, debit_by_amount, visited_pairs
            )
            
            for debit in matching_debits:
                chain = self._build_reverse_chain(
                    credit, debit, chain_id, visited_pairs
                )
                if chain:
                    self.chains.append(chain)
                    chain_id += 1
        
        logger.info(f"Built {len(self.chains)} fund flow chains")
    
    def _group_by_amount(self, transactions: List[Transaction]) -> Dict[float, List[Transaction]]:
        """Group transactions by amount for quick lookup."""
        groups = defaultdict(list)
        for txn in transactions:
            amount = round(abs(txn.amount))
            groups[amount].append(txn)
        return dict(groups)
    
    def _find_matching_credits(
        self, 
        debit_amount: float,
        debit_date: str,
        debit_party: str,
        debit: Transaction,
        credit_by_amount: Dict[float, List[Transaction]],
        visited_pairs: Set[tuple]
    ) -> List[Transaction]:
        """Find credits that could be sources for this debit."""
        matches = []
        amount = round(abs(debit_amount))
        
        for amt_key in [amount - 1, amount, amount + 1, amount - 2, amount + 2]:
            if amt_key in credit_by_amount:
                for credit in credit_by_amount[amt_key]:
                    pair = (id(credit), id(debit))
                    if pair in visited_pairs:
                        continue
                    
                    if self._is_date_proximate(debit_date, credit.date):
                        credit_party = credit.party or "Unknown"
                        if credit_party.upper() != debit_party.upper():
                            matches.append(credit)
        
        return matches[:5]
    
    def _find_matching_debits(
        self,
        credit_amount: float,
        credit_date: str,
        credit_party: str,
        credit: Transaction,
        debit_by_amount: Dict[float, List[Transaction]],
        visited_pairs: Set[tuple]
    ) -> List[Transaction]:
        """Find debits that could be recipients of this credit."""
        matches = []
        amount = round(abs(credit_amount))
        
        for amt_key in [amount - 1, amount, amount + 1, amount - 2, amount + 2]:
            if amt_key in debit_by_amount:
                for debit in debit_by_amount[amt_key]:
                    pair = (id(credit), id(debit))
                    if pair in visited_pairs:
                        continue
                    
                    if self._is_date_proximate(credit_date, debit.date):
                        debit_party = debit.party or "Unknown"
                        if debit_party.upper() != credit_party.upper():
                            matches.append(debit)
        
        return matches[:5]
    
    def _is_date_proximate(self, date1: str, date2: str, max_days: int = None) -> bool:
        """Check if two dates are within tolerance."""
        if max_days is None:
            max_days = self.DATE_TOLERANCE_DAYS
        
        try:
            d1 = self._parse_date(date1)
            d2 = self._parse_date(date2)
            
            if d1 and d2:
                delta = abs((d1 - d2).days)
                return delta <= max_days
            
            return True
            
        except:
            return True
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime."""
        if not date_str:
            return None
        
        formats = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y']
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except:
                continue
        
        return None
    
    def _build_single_chain(
        self,
        debit: Transaction,
        credit: Transaction,
        chain_id: int,
        visited_pairs: Set[tuple]
    ) -> Optional[FundFlowChain]:
        """Build a single fund flow chain."""
        pair = (id(credit), id(debit))
        visited_pairs.add(pair)
        
        credit_party = credit.party or "Unknown"
        debit_party = debit.party or "Unknown"
        
        flow_parts = [credit_party, debit_party]
        flow_path = " -> ".join(flow_parts)
        
        confidence = self._calculate_confidence(debit, credit)
        
        cross_file = 0
        if debit.source_file and credit.source_file:
            if debit.source_file != credit.source_file:
                cross_file = 1
        
        chain = FundFlowChain(
            chain_id=f"chain_{chain_id}",
            flow_path=flow_path,
            total_amount=abs(debit.amount),
            confidence=confidence,
            chain_depth=2,
            cross_pdf_links=cross_file,
            transactions=[credit, debit],
            flow_path_list=flow_parts
        )
        
        return chain
    
    def _build_reverse_chain(
        self,
        credit: Transaction,
        debit: Transaction,
        chain_id: int,
        visited_pairs: Set[tuple]
    ) -> Optional[FundFlowChain]:
        """Build a reverse fund flow chain."""
        pair = (id(credit), id(debit))
        visited_pairs.add(pair)
        
        credit_party = credit.party or "Unknown"
        debit_party = debit.party or "Unknown"
        
        flow_parts = [credit_party, debit_party]
        flow_path = " -> ".join(flow_parts)
        
        confidence = self._calculate_confidence(debit, credit)
        
        cross_file = 0
        if debit.source_file and credit.source_file:
            if debit.source_file != credit.source_file:
                cross_file = 1
        
        chain = FundFlowChain(
            chain_id=f"chain_{chain_id}",
            flow_path=flow_path,
            total_amount=abs(credit.amount),
            confidence=confidence,
            chain_depth=2,
            cross_pdf_links=cross_file,
            transactions=[credit, debit],
            flow_path_list=flow_parts
        )
        
        return chain
    
    def _calculate_confidence(self, txn1: Transaction, txn2: Transaction) -> float:
        """Calculate confidence score for a transaction pair."""
        score = 0.5
        
        amount1 = abs(txn1.amount)
        amount2 = abs(txn2.amount)
        amount_diff = abs(amount1 - amount2)
        
        if amount_diff <= 1:
            score += 0.3
        elif amount_diff <= self.AMOUNT_TOLERANCE:
            score += 0.2
        else:
            score -= 0.1
        
        if self._is_date_proximate(txn1.date, txn2.date, max_days=0):
            score += 0.1
        elif self._is_date_proximate(txn1.date, txn2.date, max_days=1):
            score += 0.05
        
        if txn1.is_transfer and txn2.is_transfer:
            score += 0.1
        
        return min(1.0, max(0.0, score))
    
    def get_chain_summary(self) -> Dict:
        """Get summary of all fund flow chains."""
        if not self.chains:
            return {
                'total_chains': 0,
                'total_amount': 0,
                'avg_chain_length': 0,
                'max_chain_depth': 0,
                'cross_file_links': 0,
                'top_chains': []
            }
        
        total_amount = sum(c.total_amount for c in self.chains)
        max_depth = max(c.chain_depth for c in self.chains)
        total_cross_file = sum(c.cross_pdf_links for c in self.chains)
        
        sorted_chains = sorted(self.chains, key=lambda c: c.total_amount, reverse=True)
        # Return all chains - frontend handles pagination (shows 10 at a time)
        top_chains = [
            {
                'chain_id': c.chain_id,
                'flow_path': c.flow_path,
                'total_amount': round(c.total_amount, 2),
                'confidence': round(c.confidence, 2)
            }
            for c in sorted_chains
        ]
        
        return {
            'total_chains': len(self.chains),
            'total_amount': round(total_amount, 2),
            'avg_chain_length': sum(c.chain_depth for c in self.chains) / len(self.chains),
            'max_chain_depth': max_depth,
            'cross_file_links': total_cross_file,
            'top_chains': top_chains
        }
    
    def get_money_path_by_party(self, party_name: str) -> List[Dict]:
        """Get money path analysis for a specific party."""
        party_name = party_name.upper()
        paths = []
        
        for chain in self.chains:
            if party_name in chain.flow_path.upper():
                paths.append({
                    'chain_id': chain.chain_id,
                    'flow_path': chain.flow_path,
                    'total_amount': round(chain.total_amount, 2),
                    'direction': 'incoming' if chain.flow_path.split(' -> ')[0].upper() == party_name else 'outgoing',
                    'confidence': round(chain.confidence, 2)
                })
        
        return paths
    
    def clear(self):
        """Clear all data."""
        self.transactions.clear()
        self.chains.clear()
        self.party_transactions.clear()
        self.source_files.clear()
