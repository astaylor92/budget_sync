import pandas as pd
import datetime as dt
import numpy as np
import os
import pickle
import os.path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pygsheets

class GSheetSynchronizer():
    def __init__(self, db, token_path, creds_path, service_path, 
                 scopes, spreadsheet_id, spreadsheet_url):
        self.db = db
        self.token_path = token_path
        self.creds_path = creds_path
        self.service_path = service_path
        self.scopes = scopes
        self.spreadsheet_id = spreadsheet_id
        self.spreadsheet_url = spreadsheet_url
        self.creds = None
        self.txn_cols = ['Date', 'Amount', 'Description', 'Account', 'Category', 'Complete', 'Key']
        self.gsheet_pull_init()
        self.gsheet_push_init()

    def gsheet_pull_init(self):
        """Checks Google Sheets API and updates token"""
        # TODO - Fix refresh error; https://github.com/googleapis/python-storage/issues/341
        self.creds = None
        try:
            if os.path.exists(self.token_path):
                with open(self.token_path, 'rb') as token:
                    self.creds = pickle.load(token)
            if not self.creds or not self.creds.valid:
                if self.creds and self.creds.expired and self.creds.refresh_token:
                    self.creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.creds_path, self.scopes)
                    self.creds = flow.run_local_server(port=0)
                with open(self.token_path, 'wb') as token:
                    pickle.dump(self.creds, token)
        except:
            os.remove(self.token_path)
            self.gsheet_pull_init()
    
    def gsheet_push_init(self):
        self.gc = pygsheets.authorize(service_file=self.service_path)
        self.sh = self.gc.open_by_url(self.spreadsheet_url)

    def pull_sheet_data(self, data_to_pull):
        """Pulls data from Google Sheet based on Spreadsheet and range"""
        service = build('sheets', 'v4', credentials=self.creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=self.spreadsheet_id,
            range=data_to_pull).execute()
        values = result.get('values', [])
        
        if not values:
            print('No data found.')
        else:
            rows = sheet.values().get(spreadsheetId=self.spreadsheet_id,
                                    range=data_to_pull,
                                    valueRenderOption='UNFORMATTED_VALUE').execute()
            data = rows.get('values')
            print("COMPLETE: Data copied")
            return data
        
    def update_gsheet_txns(self, txn_sheets, num_fields):
        """Pulls and combines all transaction google sheets"""
        # Pull down each transaction tab into a list of dataframes
        gsheet_txn_dfs = []
        for sheet in txn_sheets:
            data = self.pull_sheet_data(sheet)
            data = [d for d in data if len(d) == num_fields]
            data_arr = np.array(data[1:])
            data_arr = data_arr[[len(x) > 6 for x in data_arr]]
            if data_arr.shape[0] == 0:
                return
            df = pd.DataFrame(data_arr.tolist(), columns=data[0])
            gsheet_txn_dfs.append(df)
        
        # Append dataframes
        gsheet_txns = (
            pd.concat(gsheet_txn_dfs)
            .query('Key == Key')
            .reset_index(drop=True)
        )
        self.db.update_gsheet_txns(gsheet_txns)
    
    def push_gsheet_txns(self):
        processed_txns = pd.read_parquet(self.db.paths['processed_txns'])
        account_ref = pd.read_excel(self.db.paths['account_ref'])
        updated_txns = (
            processed_txns
            .merge(account_ref, on=['account_name_parent'])
        )
        for sheet in updated_txns['account_sheet'].dropna().unique():
            out_txns = (
                updated_txns
                .query(f"account_sheet=='{sheet}' & account_txns_include==True")
                [self.db.gsheet_txn_map.values()]
                .rename(columns = {v: k for k, v in self.db.gsheet_txn_map.items()})
                .sort_values(by='Date', axis=0, ascending=False)
                .reset_index(drop=True)
            )

            wks = self.sh.worksheet('title', sheet)
            wks.set_dataframe(out_txns[self.txn_cols], 'B4')
        pass