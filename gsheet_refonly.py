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

# ACCOUNT_REFS = pd.read_excel('./data/reference/accounts.xlsx')
# DB_GSHEET_MAP = {
#     'Date': 'txn_date',
#     'Description': 'txn_desc',
#     'Amount': 'txn_amount',
#     'Category': 'txn_cat',
#     'Account': 'account',
#     'Key': 'txn_key',
#     'Format': 'txn_cond_fmt'
# }
# GSHEET_DB_MAP = {v:k for k, v in GSHEET_TXN_MAP.items()}
# TXN_TYPES = {
#     'txn_key': 'object',
#     'txn_desc': 'object',
#     'txn_date': 'datetime64[ns]',
#     'txn_cond_fmt': 'object',
#     'txn_cat': 'object',
#     'txn_amount': 'float64',
#     'new_flag': 'bool',
#     'addl_post_date': 'object',
#     'addl_desc': 'object',
#     'addl_cat': 'object',
#     'account': 'object'
# }
# TXN_COLS = ['Date', 'Amount', 'Description', 'Account', 'Category', 'Key', 'Format']
# INC_COLS = ['Date', 'Amount', 'Key']
# NEW_TXN_FILEPATHS = {
#     'citi': '/data/transactions/new/citi/',
#     'usaa': '/data/transactions/new/usaa/',
#     'amex_andrew': '/data/transactions/new/amex_andrew/',
#     'amex_natalie': '/data/transactions/new/amex_natalie/',
#     'chase': '/data/transactions/new/chase/'
# }
# TXN_COLS_RAW = {
#     'citi': ['Date', 'Description', 'Debit', 'Credit'],
#     'usaa': ['Date', 'Description', 'Original Description', 'Category', 'Amount'],
#     'amex_andrew': ['Date', 'Description', 'Extended Details', 'Category', 'Amount'],
#     'amex_natalie': ['Date', 'Description', 'Extended Details', 'Amount'],
#     'chase': ['Transaction Date', 'Post Date', 'Description', 'Category', 'Amount']
# }
# TXN_COL_RENAMES = {
#     'citi': {
#         'Date': 'txn_date',
#         'Description': 'txn_desc',
#         'Debit': 'debit',
#         'Credit': 'credit'},
#     'usaa': {'Date': 'txn_date',
#         'Description': 'addl_desc',
#         'Original Description': 'txn_desc',
#         'Category': 'addl_cat',
#         'Amount': 'txn_amount'},
#     'amex_andrew': {
#         'Date': 'txn_date',
#         'Description': 'txn_desc',
#         'Extended Details': 'addl_desc',
#         'Category': 'addl_cat',
#         'Amount': 'txn_amount'},
#     'amex_natalie': {
#         'Date': 'txn_date',
#         'Description': 'txn_desc',
#         'Extended Details': 'addl_desc',
#         'Category': 'addl_cat',
#         'Amount': 'txn_amount'},
#     'chase': {
#         'Transaction Date': 'txn_date',
#         'Post Date': 'addl_post_date',
#         'Description': 'txn_desc',
#         'Category': 'addl_cat',
#         'Amount': 'txn_amount'},
# }


# def reset_txns():
#     """Testing only - resets transactions"""
#     pd.read_parquet('./data/transactions/transactions').iloc[3:4, :].to_parquet('./data/transactions/transactions')

class GSheetSynchronizer():
    def __init__(self, db, token_path, creds_path, scopes):
        self.db = db
        self.token_path = token_path
        self.creds_path = creds_path
        self.creds = self.gsheet_api_check(scopes, token_path, creds_path)

    def gsheet_api_check(self, scopes, token_path, creds_path):
        """Checks Google Sheets API and updates token"""
        # TODO - Fix refresh error; https://github.com/googleapis/python-storage/issues/341
        creds = None
        try:
            if os.path.exists(token_path):
                with open(token_path, 'rb') as token:
                    creds = pickle.load(token)
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        creds_path, scopes)
                    creds = flow.run_local_server(port=0)
                with open(token_path, 'wb') as token:
                    pickle.dump(creds, token)
        except:
            os.remove(token_path)
            creds = self.gsheet_api_check(scopes, token_path, creds_path)
        return creds

    def pull_sheet_data(self, spreadsheet_id, data_to_pull):
        """Pulls data from Google Sheet based on Spreadsheet and range"""
        service = build('sheets', 'v4', credentials=self.creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=data_to_pull).execute()
        values = result.get('values', [])
        
        if not values:
            print('No data found.')
        else:
            rows = sheet.values().get(spreadsheetId=spreadsheet_id,
                                    range=data_to_pull,
                                    valueRenderOption='UNFORMATTED_VALUE').execute()
            data = rows.get('values')
            print("COMPLETE: Data copied")
            return data
        
    # def push_sheet_data(data, spreadsheet_id, data_to_pull):
    #     """Pushes data from Google Sheet based on Spreadsheet and range"""
    #     service = build('sheets', 'v4', credentials=self.creds)
    #     sheet = service.spreadsheets()
    #     result = sheet.values().get(
    #         spreadsheetId=spreadsheet_id,
    #         range=data_to_pull).execute()
    #     values = result.get('values', [])
        
    #     if not values:
    #         print('No data found.')
    #     else:
    #         rows = sheet.values().get(spreadsheetId=spreadsheet_id,
    #                                 range=data_to_pull,
    #                                 valueRenderOption='UNFORMATTED_VALUE').execute()
    #         data = rows.get('values')
    #         print("COMPLETE: Data copied")
    #         return data
        
    def update_gsheet_txns(self, spreadsheet_id, txn_sheets, start_row):
        """Pulls and combines all transaction google sheets"""
        # Pull down each transaction tab into a list of dataframes
        gsheet_txn_dfs = []
        for sheet in txn_sheets:
            df = self.pull_sheet_data(spreadsheet_id, sheet, start_row=start_row)
            gsheet_txn_dfs.append(df)
        
        # Append dataframes
        gsheet_txns = (
            pd.concat(gsheet_txn_dfs)
            .reset_index(drop=True)
        )
        self.db.update_gsheet_txns(gsheet_txns)


# def gsheet_api_check(scopes):
#     """Checks Google Sheets API and updates token"""
#     # TODO - Fix refresh error; https://github.com/googleapis/python-storage/issues/341
#     creds = None
#     try:
#         if os.path.exists('./data/reference/token.pickle'):
#             with open('./data/reference/token.pickle', 'rb') as token:
#                 creds = pickle.load(token)
#         if not creds or not creds.valid:
#             if creds and creds.expired and creds.refresh_token:
#                 creds.refresh(Request())
#             else:
#                 flow = InstalledAppFlow.from_client_secrets_file(
#                     './data/reference/credentials.json', scopes)
#                 creds = flow.run_local_server(port=0)
#             with open('./data/reference/token.pickle', 'wb') as token:
#                 pickle.dump(creds, token)
#     except:
#         os.remove('./data/reference/token.pickle')
#         creds = gsheet_api_check(scopes)
#     return creds


# def pull_sheet_data(scopes, spreadsheet_id, data_to_pull):
#     """Pulls data from Google Sheet based on Spreadsheet and range"""
#     creds = gsheet_api_check(scopes)
#     service = build('sheets', 'v4', credentials=creds)
#     sheet = service.spreadsheets()
#     result = sheet.values().get(
#         spreadsheetId=spreadsheet_id,
#         range=data_to_pull).execute()
#     values = result.get('values', [])
    
#     if not values:
#         print('No data found.')
#     else:
#         rows = sheet.values().get(spreadsheetId=spreadsheet_id,
#                                   range=data_to_pull,
#                                   valueRenderOption='UNFORMATTED_VALUE').execute()
#         data = rows.get('values')
#         print("COMPLETE: Data copied")
#         return data


# def push_sheet_data(data, scopes, spreadsheet_id, data_to_pull):
#     """Pushes data from Google Sheet based on Spreadsheet and range"""
#     creds = gsheet_api_check(scopes)
#     service = build('sheets', 'v4', credentials=creds)
#     sheet = service.spreadsheets()
#     result = sheet.values().get(
#         spreadsheetId=spreadsheet_id,
#         range=data_to_pull).execute()
#     values = result.get('values', [])
    
#     if not values:
#         print('No data found.')
#     else:
#         rows = sheet.values().get(spreadsheetId=spreadsheet_id,
#                                   range=data_to_pull,
#                                   valueRenderOption='UNFORMATTED_VALUE').execute()
#         data = rows.get('values')
#         print("COMPLETE: Data copied")
#         return data


# def pull_sheet(sheet_id, data_to_pull, start_row=0):
#     """Pulls data from google sheet"""
#     scopes = ['https://www.googleapis.com/auth/spreadsheets']
#     data = pull_sheet_data(scopes, sheet_id, data_to_pull)
#     df = pd.DataFrame(data[start_row+1:], columns=data[start_row])
#     return df


# def pull_combine_gsheet_txns(spreadsheet_id, txn_sheets):
#     """Pulls and combines all transaction google sheets"""
#     # Pull down each transaction tab into a list of dataframes
#     gsheet_txn_dfs = []
#     for sheet in txn_sheets:
#         df = pull_sheet(spreadsheet_id, sheet, start_row=3)
#         gsheet_txn_dfs.append(df)
    
#     # Append dataframes
#     gsheet_txns = (
#         pd.concat(gsheet_txn_dfs)
#         .rename(columns=GSHEET_TXN_MAP)
#         .reset_index(drop=True)
#     )
#     gsheet_txns['txn_date'] = pd.TimedeltaIndex(gsheet_txns['txn_date'], unit='d') + dt.datetime(1899, 12, 30)
#     type_dict = {k:v for k, v in TXN_TYPES.items() if k in gsheet_txns.columns}
#     gsheet_txns = gsheet_txns.astype(type_dict)

#     ordered_cols = sorted(gsheet_txns.columns, reverse=True)
#     gsheet_txns['txn_desc'] = gsheet_txns['txn_desc'].astype('str')
#     gsheet_txns['txn_key'] = gsheet_txns['txn_key'].astype('str')
#     gsheet_txns[ordered_cols].to_parquet('./data/transactions/gsheet/gsheet_txns')


# def update_categories_gsheet():
#     txns = pd.read_parquet('./data/transactions/transactions')
#     gs_txns = pd.read_parquet('./data/transactions/gsheet/gsheet_txns')

#     # Write out a backup
#     today = pd.to_datetime('today').date()
#     ordered_cols = sorted(txns.columns, reverse=True)
#     txns[ordered_cols].to_parquet(f'./data/transactions/archive/{today}_1')

#     new_txns = (
#         txns
#         .merge(gs_txns[['txn_key', 'txn_cat']], how='left', on='txn_key', suffixes=['', '_new'])
#         .assign(txn_cond_fmt = lambda x: np.where(x['txn_cat_new'] != x['txn_cat'], 'Complete',  x['txn_cond_fmt']),
#                 txn_cat = lambda x: x['txn_cat_new'].fillna(x['txn_cat']))
#         .drop(columns=['txn_cat_new'])
#     )

#     ordered_cols = sorted(new_txns.columns, reverse=True)
#     new_txns[ordered_cols].to_parquet('./data/transactions/transactions')


def process_txn(account):
    """Takes in transaction file and account, and writes out processed parquet file"""
    wd = os.getcwd()+NEW_TXN_FILEPATHS[account]
    for ind, end in enumerate(os.listdir(wd)):
        filepath = wd + end
        if filepath[-4:] == ".csv":
            data = pd.read_csv(filepath)
            newdata = (
                data[TXN_COLS_RAW[account]]
                .fillna(0)
                .rename(columns=TXN_COL_RENAMES[account])
                .assign(account=account,
                        new_flag=True,
                        txn_date=lambda x: pd.to_datetime(x['txn_date']),
                        txn_cond_fmt='New',
                        txn_cat="No Category")
            )

            # Add conditional amount processing
            if account == 'citi':
                newdata = (
                    newdata
                    .assign(txn_amount=lambda x: x['credit'] + x['debit'])
                    .drop(columns=['debit', 'credit'])
                )
            elif account == 'chase':
                newdata = (
                    newdata
                    .assign(txn_amount=lambda x: -1.0*x['txn_amount'])
                )
            elif account == 'usaa':
                newdata = (
                    newdata
                    .assign(txn_amount=lambda x: -1.0*x['txn_amount'])
                )

            # Convert category to string if exists
            if 'addl_cat' in newdata.columns:
                newdata = newdata.astype({'addl_cat':'str'})

            # Add keys
            newdata = (
                newdata
                .assign(
                    txn_key_init=lambda x: x['txn_date'].astype('str') + x['txn_amount'].astype('str') \
                        + x['txn_desc'].astype('str') + x['account'].astype('str'),
                    txn_key_count=lambda x: x[['txn_key_init']].groupby('txn_key_init', as_index=False)['txn_key_init'].rank(method='dense'),
                    txn_key = lambda x: x['txn_key_init'] + '_' + x['txn_key_count'].astype('int').astype('str'))
                .drop(columns=['txn_key_init', 'txn_key_count'])
            )

            ordered_cols = sorted(newdata.columns, reverse=True)
            newdata['txn_desc'] = newdata['txn_desc'].astype('str')
            newdata['txn_key'] = newdata['txn_key'].astype('str')
            newdata[ordered_cols].to_parquet(f'./data/transactions/processed/{account}')


def process_txn_files():
    """Runs all transaction file updates"""
    # process_usaa_txn()
    # process_chase_txn()
    # process_amex_a_txn()
    # process_amex_n_txn()
    # process_citi_txn()
    process_txn('usaa')
    process_txn('chase')
    process_txn('amex_andrew')
    process_txn('amex_natalie')
    process_txn('citi')


def add_processed_txns():
    """Backs up transactions, then updates with the processed transaction files"""
    processed_txn_dfs = []
    for file in os.listdir('./data/transactions/processed'):
        if file[:3] != ".DS":
            df = pd.read_parquet('./data/transactions/processed/'+file)
            processed_txn_dfs.append(df)
    
    processed_txns = pd.concat(processed_txn_dfs)

    txns = pd.read_parquet('./data/transactions/transactions')

    # Write out a backup
    today = pd.to_datetime('today').date()
    txns.to_parquet(f'./data/transactions/archive/{today}')

    # Combine and remove dupes
    new_transactions = (
        pd.concat([txns, processed_txns])
        .reset_index(drop=True)
        .drop_duplicates('txn_key', keep='first', ignore_index=True)
    )

    ordered_cols = sorted(new_transactions.columns, reverse=True)
    new_transactions[ordered_cols].to_parquet('./data/transactions/transactions')


def add_processed_inc():
    """Backs up income, then updates with the processed income file"""
    processed_inc_dfs = []
    for file in os.listdir('./data/income/processed'):
        if file[:3] != ".DS":
            df = pd.read_parquet('./data/income/processed/'+file)
            processed_inc_dfs.append(df)
    
    processed_inc = pd.concat(processed_inc_dfs)

    inc = pd.read_parquet('./data/income/income')

    # Write out a backup
    today = pd.to_datetime('today').date()
    inc.to_parquet(f'./data/income/archive/{today}_2')

    # Combine and remove dupes
    new_inc = (
        pd.concat([inc, processed_inc])
        .reset_index(drop=True)
        .drop_duplicates('txn_key', keep='first', ignore_index=True)
    )

    ordered_cols = sorted(new_inc.columns, reverse=True)
    new_inc[ordered_cols].to_parquet('./data/income/income')


def push_new_txns():
    txns = pd.read_parquet('./data/transactions/transactions')
    
    # creds = service_account.Credentials.from_service_account_file('./data/reference/credentials.json')
    gc = pygsheets.authorize(service_file='./data/reference/service_account_key.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1wBUTPjrgRRtQr0AdqsBwb9VckssfuiQRVBrds70Jp-g/edit#gid=1784836909')

    txn_merged = txns.merge(ACCOUNT_REFS[['account', 'account_type']], on='account')

    # Update Andrew's transactions
    a_txns = (
        txn_merged
        .query('account_type=="Andrew"')
        .drop(columns='account_type')
        .rename(columns=DOWNLOAD_TXN_MAP)
        .sort_values(by=['Key'], ascending=False)
        .reset_index(drop=True)
    )

    a_wks = sh.worksheet('title', '2_Andrew_Transactions')
    a_wks.set_dataframe(a_txns[TXN_COLS], 'B4')

    # Update Nat's transactions
    n_txns = (
        txn_merged
        .query('account_type=="Natalie"')
        .drop(columns='account_type')
        .rename(columns=DOWNLOAD_TXN_MAP)
        .sort_values(by=['Key'], ascending=False)
        .reset_index(drop=True)
    )

    n_wks = sh.worksheet('title', '2_Natalie_Transactions')
    n_wks.set_dataframe(n_txns[TXN_COLS], 'B4')

    # Update joint transactions
    j_txns = (
        txn_merged
        .query('account_type=="Joint"')
        .drop(columns='account_type')
        .rename(columns=DOWNLOAD_TXN_MAP)
        .sort_values(by=['Key'], ascending=False)
        .reset_index(drop=True)
    )

    j_wks = sh.worksheet('title', '2_Joint_Transactions')
    j_wks.set_dataframe(j_txns[TXN_COLS], 'B4')


def push_new_inc():
    inc = pd.read_parquet('./data/income/income')
    
    # creds = service_account.Credentials.from_service_account_file('./data/reference/credentials.json')
    gc = pygsheets.authorize(service_file='./data/reference/service_account_key.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1wBUTPjrgRRtQr0AdqsBwb9VckssfuiQRVBrds70Jp-g/edit#gid=1784836909')

    # Update income
    inc = (
        inc
        .rename(columns=DOWNLOAD_TXN_MAP)
        .sort_values(by=['Key'], ascending=False)
        .reset_index(drop=True)
    )

    wks = sh.worksheet('title', '2_Joint_Income')
    wks.set_dataframe(inc[INC_COLS], 'B4')


def process_balances():
    start_date = '2022-12-20'
    bal_dfs = []
    for folder in os.listdir('./data/balances/new'):
        if folder[:3] != ".DS":
            for file in os.listdir('./data/balances/new/'+folder):
                if file == "balance.xlsm":
                    df = (
                        pd.read_excel('./data/balances/new/'+folder+'/'+file)
                        .assign(account=folder)
                        .rename(columns={'date':'bal_date'})
                    )
                    bal_dfs.append(df)

    new_bal_df = (
        pd.concat(bal_dfs)
        .merge(ACCOUNT_REFS, on='account')
        .assign(balance = lambda x: np.where(x['balance_type']=="CC", -1.0*x['balance'], x['balance']))
        .drop(columns=['account_type', 'account_name', 'balance_type'])
        .drop_duplicates(subset=['bal_date', 'account'], keep='first')
    )

    balances = pd.read_parquet('./data/balances/balances')
    balances = (
        pd.concat([balances, new_bal_df])
        .drop_duplicates(subset=['bal_date', 'account'], keep='first')
    )

    latest = new_bal_df['bal_date'].max().strftime('%Y-%m-%d')
    dates = pd.DataFrame({'bal_date': pd.date_range(start_date, latest)})
    date_accts = dates.merge(ACCOUNT_REFS[['account']], how='cross')
    balances = (
        date_accts
        .merge(balances, how='left', on=['bal_date', 'account'])
        .sort_values(by=['account', 'bal_date'])
        .fillna(method='ffill')
    )

    
    balances.to_parquet('./data/balances/balances')


def update_balance_gsheet():
    balances = pd.read_parquet('./data/balances/balances')

    gc = pygsheets.authorize(service_file='./data/reference/service_account_key.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1wBUTPjrgRRtQr0AdqsBwb9VckssfuiQRVBrds70Jp-g/edit#gid=1784836909')

    # Update balances
    b1 = (
        balances
        .merge(ACCOUNT_REFS, on='account')
        [['account', 'bal_date', 'balance', 'balance_type']]
        .rename(columns={'account': 'Account',
                         'bal_date': 'Date',
                         'balance': 'Balance',
                         'balance_type': 'Account Type'})
    )

    # Update gsheet
    b1_wks = sh.worksheet('title', '99_Balance_Detail')
    b1_wks.set_dataframe(b1, 'A1')

    # Update Checking + Savings - CC
    types = ['Savings', 'Checking', 'CC']
    b2 = (
        b1
        .query(f'`Account Type`.isin({types})')
        [['Date', 'Balance']]
        .groupby(by=['Date'], as_index=False)
        .sum()
    )

    # Update gsheet
    b2_wks = sh.worksheet('title', '99_Balance_Summaries')
    b2_wks.set_dataframe(b2, 'A2')

    # Update HSA
    types = ['HSA']
    b3 = (
        b1
        .query(f'`Account Type`.isin({types})')
        [['Date', 'Balance']]
        .groupby(by=['Date'], as_index=False)
        .sum()
    )

    # Update gsheet
    b3_wks = sh.worksheet('title', '99_Balance_Summaries')
    b3_wks.set_dataframe(b3, 'D2')
    
    # Update Retirement
    types = ['Retirement']
    b4 = (
        b1
        .query(f'`Account Type`.isin({types})')
        [['Date', 'Balance']]
        .groupby(by=['Date'], as_index=False)
        .sum()
    )

    # Update gsheet
    b4_wks = sh.worksheet('title', '99_Balance_Summaries')
    b4_wks.set_dataframe(b4, 'G2')

    # Update Checking - CC
    types = ['Checking', 'CC']
    b5 = (
        b1
        .query(f'`Account Type`.isin({types})')
        [['Date', 'Balance']]
        .groupby(by=['Date'], as_index=False)
        .sum()
    )

    # Update gsheet
    b5_wks = sh.worksheet('title', '99_Balance_Summaries')
    b5_wks.set_dataframe(b5, 'J2')

    # TODO - Finish updating

    pass


def sync_gsheet_data():
    # Pull gsheet transactions, append to each other, store in a temporary parquet file
    spreadsheet_id = "1wBUTPjrgRRtQr0AdqsBwb9VckssfuiQRVBrds70Jp-g"
    txn_sheets = ["'2_Andrew_Transactions'!B:H", "'2_Natalie_Transactions'!B:H", "'2_Joint_Transactions'!B:H"]
    pull_combine_gsheet_txns(spreadsheet_id, txn_sheets)

    # Backup the processed_transactions file and update categories from gsheet data
    update_categories_gsheet()

    # Push transactions back up to the cloud

if __name__ == "__main__":

    ## TRANSACTIONS

    # Pull, process, combine gsheet transactions
    spreadsheet_id = "1wBUTPjrgRRtQr0AdqsBwb9VckssfuiQRVBrds70Jp-g"
    txn_sheets = ["'2_Andrew_Transactions'!B:H", "'2_Natalie_Transactions'!B:H", "'2_Joint_Transactions'!B:H"]
    pull_combine_gsheet_txns(spreadsheet_id, txn_sheets)

    # Update categories from gsheets
    update_categories_gsheet()

    # Retrain prediction model
    
    # Process live transaction files
    process_txn_files()

    # Predict the category

    # Add these records to the source of truth
    add_processed_txns()
    add_processed_inc()

    # Push transactions back up
    push_new_txns()
    push_new_inc()

    ## BALANCES

    # Process new balance files
    process_balances()

    # Update gsheets
    update_balance_gsheet()

    pass