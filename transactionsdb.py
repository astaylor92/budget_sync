#!python3

import sqlite3
import json
import datetime
import pandas as pd
import numpy as np
from typing import List, Optional, Dict
from plaidapi import AccountBalance, AccountInfo, Transaction as PlaidTransaction
from Levenshtein import distance

def build_placeholders(list):
    return ",".join(["?"]*len(list))

class TransactionsDB():
    def __init__(self, dbfolder: str):
        self.dbfolder = dbfolder if dbfolder[-1] != '/' else dbfolder[:-1]
        self.paths = {
            'account_ref': self.dbfolder + '/accounts.xlsx',
            'raw_txns': self.dbfolder + '/raw_transactions.parquet',
            'raw_txns_backup': self.dbfolder + '/backup/raw_transactions.parquet',
            'cat_data': self.dbfolder + '/cat_data.parquet',
            'cat_data_backup': self.dbfolder + '/backup/cat_data.parquet',
            'gsheet_txns': self.dbfolder + '/gsheet_transactions.parquet',
            'gsheet_txns_backup': self.dbfolder + '/backup/gsheet_transactions.parquet',
            'processed_txns': self.dbfolder + '/processed_transactions.parquet',
            'processed_txns_backup_1': self.dbfolder + '/backup/processed_transactions_1.parquet',
            'processed_txns_backup_2': self.dbfolder + '/backup/processed_transactions_2.parquet',
            'processed_txns_backup_3': self.dbfolder + '/backup/processed_transactions_3.parquet',
            'raw_balances': self.dbfolder + '/raw_balances.parquet',
            'raw_balances_backup': self.dbfolder + '/backup/raw_balances.parquet',
            'processed_balances': self.dbfolder + '/processed_balances.parquet',
            'processed_balances_backup': self.dbfolder + '/backup/processed_balances.parquet',
            'account_info': self.dbfolder + '/account_info.parquet',
            'account_info_backup': self.dbfolder + '/backup/account_info.parquet'
        }
        self.gsheet_txn_map = {
            'Date': 'txn_date',
            'Description': 'txn_name',
            'Amount': 'txn_amount',
            'Category': 'txn_cat',
            'Account': 'account_name_parent',
            'Complete': 'txn_cat_flag',
            'Key': 'txn_id',
        }
        self.txn_types = {
            'account_id': 'object',
            'account_name': 'object',
            'account_name_parent': 'object',
            'txn_id': 'object',
            'txn_date': 'datetime64[ns]',
            'txn_name': 'object',
            'txn_name_plaid': 'object',
            'txn_amount': 'float64',
            'txn_cat': 'object',
            'txn_cat_plaid': 'object',
            'txn_cat_plaid_dtl': 'object',
            'create_dt': 'datetime64[ns]',
            'archive_dt': 'datetime64[ns]',
            'txn_cat_flag': 'bool',
            'current': 'object',
            'raw_data': 'object'
        }
        self.bal_types = {
            'account_id': 'object', 
            'account_name': 'object', 
            'bal_available': 'float64', 
            'bal_limit': 'float64', 
            'bal_currency_code': 'object', 
            'bal_current': 'float64', 
            'bal_date': 'datetime64[ns]', 
            'raw_data': 'object'
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
    
    def save_transactions_csv(self, csv_path):
        new_txns = pd.read_csv(csv_path)
        new_txns['archive_dt'] = np.nan
        new_txns['current'] = True
        new_txns = new_txns.astype({k: v for k, v in self.txn_types.items() if k in new_txns.columns})

        try:
            existing_txns = pd.read_parquet(self.paths['raw_txns'])
            existing_txns.to_parquet(self.paths['raw_txns_backup'])
            out_txns = pd.concat([existing_txns, new_txns], ignore_index=True)
        except:
            out_txns = new_txns
        
        out_txns.to_parquet(self.paths['raw_txns'])

    def save_transactions(self, transactions: PlaidTransaction):
        cols = ['account_id', 'txn_id', 'txn_date', 'txn_name', 'txn_name_plaid', 'txn_amount', 'txn_cat_plaid', 
                'txn_cat_plaid_dtl', 'create_dt', 'raw_data']
        vals = [[getattr(t, col) for t in transactions] for col in cols]
        new_txns = pd.DataFrame(dict(zip(cols, vals)))
        new_txns['archive_dt'] = np.nan
        new_txns['current'] = True
        new_txns = new_txns.astype({k: v for k, v in self.txn_types.items() if k in new_txns.columns})

        try:
            existing_txns = pd.read_parquet(self.paths['raw_txns'])
            existing_txns.to_parquet(self.paths['raw_txns_backup'])
            out_txns = pd.concat([existing_txns, new_txns], ignore_index=True)
        except:
            out_txns = new_txns
        
        out_txns.to_parquet(self.paths['raw_txns'])

    def process_transactions(self):
        raw_txns = pd.read_parquet(self.paths['raw_txns'])
        acct_names = pd.read_parquet(self.paths['account_info'])[['account_id', 'account_name_parent', 'account_name']]
        raw_txn_adj = (
                raw_txns[['account_id', 'txn_id', 'txn_name', 'txn_date', 'txn_amount']]
                .merge(acct_names, how='inner', on=['account_id'])
                .assign(txn_cat='')
                .assign(txn_cat_flag=False)
            )
        raw_txn_adj = raw_txn_adj.astype({k: v for k, v in self.txn_types.items() if k in raw_txn_adj.columns})
        try:
            processed_txns = pd.read_parquet(self.paths['processed_txns'])
            processed_txns.to_parquet(self.paths['processed_txns_backup_1'])
            out_txns = pd.concat([raw_txn_adj[~raw_txn_adj['txn_id'].isin(processed_txns['txn_id'])], processed_txns])
        except:
            out_txns = raw_txn_adj
        
        out_txns.to_parquet(self.paths['processed_txns'])

    def update_gsheet_txns(self, gsheet_txns):
        gsheet_txns = (
            gsheet_txns
            .rename(columns=self.gsheet_txn_map)
            .assign(txn_cat_flag=lambda x: x['txn_cat_flag'].map({'True':True, 'False':False}))
            .astype({k:v for k, v in self.txn_types.items() if k in gsheet_txns.columns})
        )
        gsheet_txns['txn_date'] = pd.TimedeltaIndex(gsheet_txns['txn_date'].astype('int64'), unit='d') + datetime.datetime(1899, 12, 30)
        gsheet_txns['txn_name'] = gsheet_txns['txn_name'].astype('str')
        gsheet_txns['txn_id'] = gsheet_txns['txn_id'].astype('str')
        try:
            prev_gsheet_txns = pd.read_parquet(self.paths['gsheet_txns'])
            prev_gsheet_txns.to_parquet(self.paths['gsheet_txns_backup'])
            out_txns = pd.concat([prev_gsheet_txns[~prev_gsheet_txns['txn_id'].isin(gsheet_txns['txn_id'])], gsheet_txns])
        except:
            out_txns = gsheet_txns

        out_txns.to_parquet(self.paths['gsheet_txns'])

    def sync_categories(self):
        try:
            gsheet_txns = pd.read_parquet(self.paths['gsheet_txns']).rename(columns={'txn_cat': 'txn_cat_new', 'txn_cat_flag':'txn_cat_flag_new'})
        except:
            print("No categories synced - no gsheet data")
            return
        processed_txns = pd.read_parquet(self.paths['processed_txns'])
        processed_txns.to_parquet(self.paths['processed_txns_backup_2'])
        out_txns = (
            processed_txns
            .merge(gsheet_txns[['txn_id', 'txn_cat_new', 'txn_cat_flag_new']], how='left', on=['txn_id'])
            .assign(txn_cat_out = lambda x: x['txn_cat_new'].fillna(x['txn_cat']))
            .assign(txn_cat_flag_out = lambda x: x['txn_cat_flag_new'].fillna(x['txn_cat_flag']))
            .drop(columns=['txn_cat', 'txn_cat_new', 'txn_cat_flag', 'txn_cat_flag_new'])
            .rename(columns={'txn_cat_out': 'txn_cat', 'txn_cat_flag_out': 'txn_cat_flag'})
        )
        out_txns.to_parquet(self.paths['processed_txns'])

    def update_training_data(self):
        processed_txns = pd.read_parquet(self.paths['processed_txns'])
        new_cat_data = (
            processed_txns
            .query("txn_cat_flag==True")
            [['txn_id', 'txn_name', 'txn_cat']]
        )
        try:
            cat_data = pd.read_parquet(self.paths['cat_data'])
            cat_data.to_parquet(self.paths['cat_data_backup'])
            out_cat = pd.concat([cat_data, new_cat_data]).drop_duplicates(subset=['txn_id'])
        except:
            out_cat = new_cat_data

        out_cat.to_parquet(self.paths['cat_data'])

    def save_account_info_csv(self, csv_path):
        new_info = pd.read_csv(csv_path)

        try:
            existing_info = pd.read_parquet(self.paths['account_info'])
            existing_info.to_parquet(self.paths['account_info_backup'])
            out_info = pd.concat([existing_info, new_info], ignore_index=True)
            out_info = out_info.drop_duplicates(subset=['account_id'])
        except:
            out_info = new_info      
        
        out_info.to_parquet(self.paths['account_info'])

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

    def predict_categories(self):
        cat_data = pd.read_parquet(self.paths['cat_data'])
        processed_txns = pd.read_parquet(self.paths['processed_txns'])
        processed_txns.to_parquet(self.paths['processed_txns_backup_3'])

        if processed_txns.query("txn_cat=='' & txn_cat_flag==False").shape[0] > 0:
            data = (
                processed_txns
                .query("txn_cat=='' & txn_cat_flag==False")
                .assign(keycol=1)
                [['txn_id', 'txn_name', 'keycol']]
                .merge(
                    cat_data.rename(columns={'txn_name':'txn_name_r', 'txn_id':'txn_id_r'}).assign(keycol=1),
                    how='inner',
                    left_on='keycol',
                    right_on='keycol'
                )
                .assign(distance = lambda x: x.apply(lambda x: distance(x.txn_name, x.txn_name_r), axis=1))
                .assign(rank=lambda x: x.groupby('txn_id')['distance'].rank(method='first', ascending=True))
                .query('rank <= 10')
                .groupby('txn_id')
                .agg({'txn_name': 'first', 'txn_cat': pd.Series.mode})
                .reset_index()
                .assign(txn_cat_new = lambda x: x['txn_cat'].apply(lambda x: x[0] if type(x) is np.ndarray else x))
                .drop(columns=['txn_cat'])
            )

            out_txns = (
                processed_txns
                .merge(data[['txn_id', 'txn_cat_new']], on=['txn_id'], how='left')
                .assign(txn_cat_out = lambda x: x['txn_cat_new'].fillna(x['txn_cat']))
                .drop(columns=['txn_cat', 'txn_cat_new'])
                .rename(columns={'txn_cat_out':'txn_cat'})
            )

            out_txns.to_parquet(self.paths['processed_txns'])
        
        else:
            print("No records to predict categories for")
            return

    def save_balances(self, balances: AccountBalance):
        cols = ['account_id', 'account_name', 'bal_available', 'bal_limit', 'bal_currency_code', 'bal_current', 'bal_date', 'raw_data']
        vals = [[getattr(b, col) for b in balances] for col in cols]
        new_bals = pd.DataFrame(dict(zip(cols, vals)))
        new_bals = new_bals.astype({k: v for k, v in self.bal_types.items() if k in new_bals.columns})

        try:
            existing_bals = pd.read_parquet(self.paths['raw_balances'])
            existing_bals.to_parquet(self.paths['raw_balances_backup'])
            out_bals = pd.concat([existing_bals, new_bals], ignore_index=True)
            out_bals = out_bals.drop_duplicates(subset=['account_id', 'bal_date'])
        except:
            out_bals = new_bals

        out_bals.to_parquet(self.paths['raw_balances'])

    def process_balances(self, start_date):
        # Read in account info
        account_info = pd.read_parquet(self.paths['account_info'])

        # Read in raw balances
        raw_bals = pd.read_parquet(self.paths['raw_balances'])
        raw_bals_latest = (
            raw_bals
            .merge(account_info[['account_id', 'account_name_parent']],
                   on='account_id',
                   how='inner')
            [['account_name_parent', 'bal_current', 'bal_date']]
            .groupby(['account_name_parent', 'bal_date'])
            .agg({'bal_current':'sum'}).reset_index()
            .assign(rank=lambda x: x.groupby('account_name_parent')['bal_date'].rank(method='first', ascending=True))
            .query('rank==1')
            .drop(columns=['rank'])
        )

        # Read in all transactions, summarize by day and parent account
        processed_txns = pd.read_parquet(self.paths['processed_txns'])
        dates = pd.DataFrame({'txn_date': pd.date_range(start_date, datetime.datetime.today())})
        account_dates = (
            dates.assign(keycol=1)
            .merge(processed_txns.assign(keycol=1)[['account_name_parent', 'keycol']].drop_duplicates(),
                   on='keycol',
                   how='inner')
            .drop(columns=['keycol'])
        )
        processed_txns_byday = (
            processed_txns
            .groupby(['account_name_parent', 'txn_date'])
            .agg({'txn_amount': 'sum'})
            .reset_index()
            .merge(account_dates, on=['account_name_parent', 'txn_date'], how='right')
            .fillna(value={'txn_amount': 0})
            .rename(columns={'txn_date':'bal_date'})
        )

        # Do cumulative math to get the historical view of balances by day by parent account, back to start_date
        processed_bals_new = (
            raw_bals_latest
            .merge(processed_txns_byday, on=['bal_date', 'account_name_parent'], how='outer')
            .fillna({'bal_current':0, 'txn_amount': 0})
            .sort_values(by=['account_name_parent', 'bal_date'], ascending=False, axis=0)
            .assign(new_bal=lambda x: x['bal_current'] + x['txn_amount'])
            .assign(out_bal=lambda x: x.groupby('account_name_parent')['new_bal'].cumsum())
            .drop(columns=['new_bal', 'txn_amount', 'bal_current'])
            .rename(columns={'out_bal':'bal_processed'})
            .reset_index(drop=True)
        )

        # Append to processed bals, create if doesn't exist
        try:
            processed_bals = pd.read_parquet(self.paths['processed_balances'])
            processed_bals.to_parquet(self.paths['processed_balances_backup'])
            out_bals = pd.concat([processed_bals, processed_bals_new]).drop_duplicates(['account_name_parent', 'bal_date'])
        except:
            out_bals = processed_bals_new

        out_bals.to_parquet(self.paths['processed_balances'])

    def process_bal_summaries(self):
        processed_bals = pd.read_parquet(self.paths['processed_balances'])
        pass