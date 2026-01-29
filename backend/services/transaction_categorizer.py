"""
Transaction Categorization Service
Uses NLP-based classification to categorize transactions
"""

import re
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

# --------------------------------------------------
# SAFE FLOAT HELPER (CRITICAL FIX)
# --------------------------------------------------

def safe_float(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class TransactionCategorizer:
    """Categorize transactions using NLP-based rules and patterns"""
    
    def __init__(self):
        self.category_patterns = self._build_category_patterns()
        self.merchant_risk_keywords = self._build_risk_keywords()
    
    def _build_category_patterns(self) -> Dict[str, List[str]]:
        return {
            'Income': [
                r'salary', r'payroll', r'wages', r'income', r'credit.*salary',
                r'employer', r'pay.*credit', r'salary.*credit', r'salary.*transfer'
            ],
            'Reward/Cashback': [
                r'reward', r'cashback', r'bonus', r'cash.*back', r'loyalty',
                r'points.*credit', r'reward.*points', r'cashback.*credit'
            ],
            'Refund': [
                r'refund', r'reversal', r'chargeback', r'return.*refund',
                r'refund.*credit', r'payment.*reversal', r'refund.*received'
            ],
            'Bill Payment': [
                r'electricity', r'water', r'gas', r'utility', r'bill.*payment',
                r'phone.*bill', r'mobile.*bill', r'internet.*bill', r'cable.*bill'
            ],
            'Subscription': [
                r'subscription', r'netflix', r'spotify', r'prime', r'monthly.*fee',
                r'annually', r'recurring.*subscription', r'auto.*debit.*subscription',
                r'amazon.*prime', r'youtube.*premium'
            ],
            'EMI': [
                r'emi', r'loan.*emi', r'installment', r'loan.*repayment',
                r'equated.*monthly', r'home.*loan.*emi', r'car.*loan.*emi'
            ],
            'UPI Transfer': [
                r'upi', r'paytm', r'phonepe', r'gpay', r'google.*pay',
                r'upi.*transfer', r'upi.*payment'
            ],
            'Bank Transfer': [
                r'neft', r'rtgs', r'imps', r'bank.*transfer',
                r'online.*transfer', r'electronic.*transfer'
            ],
            'Cash Flow': [
                r'atm', r'cash.*withdrawal', r'cash.*atm',
                r'withdrawal.*atm', r'cash.*deposit'
            ],
            'Loan': [
                r'loan.*disbursement', r'personal.*loan',
                r'loan.*credit', r'loan.*disbursed'
            ],
            'Investment': [
                r'investment', r'mutual.*fund', r'stocks', r'shares',
                r'fixed.*deposit', r'fd', r'rd', r'sip.*investment'
            ],
            'Expense': [
                r'expense', r'purchase', r'payment', r'debit',
                r'pos.*transaction', r'card.*payment'
            ],
        }
    
    def _build_risk_keywords(self) -> Dict[str, float]:
        return {
            'high_risk': ['casino', 'gambling', 'betting', 'crypto', 'bitcoin', 'forex'],
            'medium_risk': ['online.*payment', 'international', 'foreign.*transaction'],
            'low_risk': ['salary', 'utility', 'government', 'bank']
        }
    
    def categorize_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        description = str(transaction.get('description', '')).lower()

        credit = safe_float(transaction.get('credit'))
        debit = safe_float(transaction.get('debit'))
        amount = credit if credit > 0 else debit
        
        category = 'Unknown'
        subcategory = ''
        merchant_risk_score = 0.5
        narration_risk_confidence = 0.5
        behavioral_deviation = 'Normal'
        
        matched_category = None
        max_match_length = 0
        
        for cat_key, patterns in self.category_patterns.items():
            for pattern in patterns:
                if re.search(pattern, description, re.IGNORECASE):
                    if len(pattern) > max_match_length:
                        max_match_length = len(pattern)
                        matched_category = cat_key
                        break
        
        if matched_category:
            category = matched_category
        else:
            if credit > 0:
                category = 'Income'
            elif debit > 0:
                category = 'Expense'
        
        merchant_risk_score = self._calculate_merchant_risk(description)
        
        if matched_category:
            narration_risk_confidence = min(0.95, 0.5 + (max_match_length / 100))
        else:
            narration_risk_confidence = 0.3
        
        behavioral_deviation = self._determine_behavioral_deviation(transaction, category)
        
        return {
            'category': category,
            'subcategory': subcategory,
            'merchant_risk_score': round(merchant_risk_score, 3),
            'narration_risk_confidence': round(narration_risk_confidence, 3),
            'behavioral_deviation': behavioral_deviation
        }
    
    def _calculate_merchant_risk(self, description: str) -> float:
        description = description.lower()
        
        for keyword in self.merchant_risk_keywords['high_risk']:
            if re.search(keyword, description):
                return 0.9
        
        for keyword in self.merchant_risk_keywords['medium_risk']:
            if re.search(keyword, description):
                return 0.6
        
        for keyword in self.merchant_risk_keywords['low_risk']:
            if re.search(keyword, description):
                return 0.2
        
        return 0.5
    
    def _determine_behavioral_deviation(self, transaction: Dict[str, Any], category: str) -> str:
        amount = safe_float(transaction.get('credit')) or safe_float(transaction.get('debit'))
        
        if amount > 100000:
            return 'High Value'
        elif amount < 10:
            return 'Micro Transaction'
        elif category == 'Unknown':
            return 'Uncategorized'
        else:
            return 'Normal'
