#!python3

import sqlite3
import json
import datetime
import pandas as pd
import numpy as np
from typing import List, Optional, Dict
from plaidapi import AccountBalance, AccountInfo, Transaction as PlaidTransaction

# TODO - Specify the tabel schema

def build_placeholders(list):
    return ",".join(["?"]*len(list))

class TransactionsDB():
    def __init__(self, dbfolder: str):
        self.dbfolder = dbfolder if dbfolder[-1] != '/' else dbfolder[:-1]
        self.paths = {
            'txns': self.dbfolder + '/raw_transactions.parquet',
            'txns_backup': self.dbfolder + '/backup/raw_transactions.parquet',
            'balances': self.dbfolder + '/raw_balances.parquet',
            'balances_backup': self.dbfolder + '/backup/raw_balances.parquet',
            'account_info': self.dbfolder + '/account_info.parquet',
            'account_info_backup': self.dbfolder + '/backup/account_info.parquet'
        }

    def get_transaction_ids(self, start_date: datetime.date, end_date: datetime.date, account_ids: List[str]) -> List[str]:
        try:
            txns = pd.read_parquet(self.paths['txns'])
            txn_ids = (
                txns
                [txns['account_id'].isin(account_ids)]
                ['txn_id'].tolist()
            )
        except:
            txn_ids = []
        return txn_ids

    def save_transactions(self, transactions: PlaidTransaction):
        cols = ['account_id', 'txn_id', 'txn_date', 'txn_name', 'txn_name_plaid', 'txn_amount', 'txn_cat_plaid', 
                'txn_cat_plaid_dtl', 'create_dt', 'raw_data']
        vals = [[getattr(t, col) for t in transactions] for col in cols]
        new_txns = pd.DataFrame(dict(zip(cols, vals)))
        new_txns['archive_dt'] = np.nan
        new_txns['current'] = True

        try:
            existing_txns = pd.read_parquet(self.paths['txns'])
            existing_txns.to_parquet(self.paths['txns_backup'])
            out_txns = pd.concat([existing_txns, new_txns], ignore_index=True)
        except:
            out_txns = new_txns
        
        out_txns.to_parquet(self.paths['txns'])

    def save_account_info(self, account_info: AccountInfo):
        cols = ['account_id', 'account_name', 'account_name_parent', 'account_name_ofcl', 'account_type', 'account_subtype', 'account_number']
        vals = [[getattr(ai, col) for ai in account_info] for col in cols]
        new_info = pd.DataFrame(dict(zip(cols, vals)))

        try:
            existing_info = pd.read_parquet(self.paths['account_info'])
            existing_info.to_parquet(self.paths['account_info_backup'])
            out_info = pd.concat([existing_info, new_info], ignore_index=True)
            out_info = out_info.drop_duplicates(subset=['account_id'])
        except:
            out_info = new_info      
        
        out_info.to_parquet(self.paths['account_info'])

    def save_balances(self, balances: AccountBalance):
        cols = ['account_id', 'account_name', 'bal_available', 'bal_limit', 'bal_currency_code', 'bal_current', 'bal_date', 'raw_data']
        vals = [[getattr(b, col) for b in balances] for col in cols]
        new_bals = pd.DataFrame(dict(zip(cols, vals)))

        try:
            existing_bals = pd.read_parquet(self.paths['balances'])
            existing_bals.to_parquet(self.paths['balances_backup'])
            out_bals = pd.concat([existing_bals, new_bals.astype], ignore_index=True)
            out_bals = out_bals.drop_duplicates(subset=['account_id', 'bal_date'])
        except:
            out_bals = new_bals

        out_bals.to_parquet(self.paths['balances'])