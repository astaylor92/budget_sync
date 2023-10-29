#!python3

import sqlite3
import json
import datetime
from typing import List, Optional, Dict
from plaidapi import AccountBalance, AccountInfo, Transaction as PlaidTransaction

# TODO - check primary key on transactions
# TODO - get rid of archive

def build_placeholders(list):
    return ",".join(["?"]*len(list))

class TransactionsDB():
    def __init__(self, dbfolder:str):
        self.dbfolder = dbfolder

    def get_transaction_ids(self, start_date: datetime.date, end_date: datetime.date, account_ids: List[str]) -> List[str]:
        c = self.conn.cursor()
        res = c.execute("""
                select transaction_id from transactions
                where json_extract(plaid_json, '$.date') between ? and ?
                and account_id in ({PARAMS})
                and archived is null
            """.replace("{PARAMS}", build_placeholders(account_ids)),
            [start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")] + list(account_ids)
        )
        return [r[0] for r in res.fetchall()]
    
    def check_transaction_head(self):
        c = self.conn.cursor()
        res = c.execute("""
                        select * from transactions limit 10
                        """)
        return res

    def archive_transactions(self, transaction_ids: List[str]):
        c = self.conn.cursor()
        c.execute("""
                update transactions set archived = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                where archived is null
                and transaction_id in ({PARAMS})
                """.replace("{PARAMS}", build_placeholders(transaction_ids)),
                  list(transaction_ids)
                  )

        self.conn.commit()

    def save_transaction(self, transaction: PlaidTransaction):
        c = self.conn.cursor()
        c.execute("""
            insert into
                transactions(account_id, transaction_id, created, updated, archived, plaid_json)
                values(?,?,strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),null,?)
                on conflict(account_id, transaction_id) DO UPDATE
                    set updated    = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                        plaid_json = excluded.plaid_json
        """, [transaction.account_id, transaction.transaction_id, json.dumps(transaction.raw_data, default=str)])

        self.conn.commit()

    def save_item_info(self, item_info: AccountInfo):
        c = self.conn.cursor()

        c.execute("""
            insert into
                items(item_id, institution_id, consent_expiration, last_failed_update, last_successful_update, updated, plaid_json)
                values(?,?,?,?,?,strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),?)
                on conflict(item_id) DO UPDATE
                    set updated    = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                    institution_id = excluded.institution_id,
                    consent_expiration = excluded.consent_expiration,
                    last_failed_update = excluded.last_failed_update,
                    last_successful_update = excluded.last_successful_update,
                    plaid_json = excluded.plaid_json
        """, [item_info.item_id, item_info.institution_id, item_info.ts_consent_expiration, item_info.ts_last_failed_update, 
              item_info.ts_last_successful_update, json.dumps(item_info.raw_data, default=str)])

        self.conn.commit()

    def save_balance(self, balance: list):
        # Create pandas table with balances

        # Load existing file and append if available

        # Write out to parquet

        pass

    def fetch_transactions_by_id(self, transaction_ids: List[str]) -> List[PlaidTransaction]:
        c = self.conn.cursor()
        r = c.execute("""
            select plaid_json from transactions
            where transaction_id in ({PARAMS})
        """.replace("{PARAMS}", build_placeholders(transaction_ids)), list(transaction_ids))
        return [ 
            PlaidTransaction(json.loads(d[0]))
            for d in r.fetchall()
        ]
