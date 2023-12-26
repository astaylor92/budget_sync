from plaidsync import *
import os
from gsheet import *

def main():
    args = parse_options()

    default_config_win = 'G:\\My Drive\\01-Banking_Ins\\Budget_Data\\config'
    default_config_mac = '/Users/andrewtaylor/Google Drive/01-Banking_Ins/Budget_Data/config'

    # Collect configuration and options
    # args = Namespace(balances=True, check_txns=False, config_file='config/budget_config', 
    #                  end_date=datetime.date(2023,11,30), account_link=None, start_date=datetime.date(2023, 11, 1), 
    #                  account_update=None, verbose=True)
    
    if os.path.exists(args.config_file):
        config_file = args.config_file
    elif os.path.exists(default_config_win):
        token_path = default_config_win+'\\gsheet_config\\token.pickle'
        creds_path = default_config_win+'\\gsheet_config\\credentials.json'
        service_path = default_config_win+'\\gsheet_config\\service_account_key.json'
        config_file = default_config_win+'\\budget_config'
    else:
        token_path = default_config_mac+'/gsheet_config/token.pickle'
        creds_path = default_config_mac+'/gsheet_config/credentials.json'
        service_path = default_config_mac+'/gsheet_config/service_account_key.json'
        config_file = default_config_mac+'/budget_config'
    cfg = config.Config(config_file)
    dbfile_win, dbfile_mac = cfg.get_dbfiles()
    if os.path.exists(dbfile_win):
        db = transactionsdb.TransactionsDB(dbfile_win)
    else:
        db = transactionsdb.TransactionsDB(dbfile_mac)
    papi = plaidapi.PlaidAPI(**cfg.get_plaid_client_config())

    # Sync plaid data with raw_* tables
    sync_plaid_data(args.account_update, args.account_link, cfg, papi, args.start_date, args.end_date, db,
                    args.balances, args.verbose)
    
    # Add new data from raw_transactions to processed_transactions
    db.process_transactions()
    
    # Pull gsheet data and update database version
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    spreadsheet_id = "1wBUTPjrgRRtQr0AdqsBwb9VckssfuiQRVBrds70Jp-g"
    spreadsheet_url = "https://docs.google.com/spreadsheets/d/1wBUTPjrgRRtQr0AdqsBwb9VckssfuiQRVBrds70Jp-g/edit#gid=1784836909"
    txn_sheets = ["'2_Andrew_Transactions'!B:H", "'2_Natalie_Transactions'!B:H", "'2_Joint_Transactions'!B:H"]
    start_row = 3
    gs = GSheetSynchronizer(db, token_path, creds_path, service_path, scopes, spreadsheet_id, spreadsheet_url)
    gs.update_gsheet_txns(txn_sheets=txn_sheets, start_row=start_row)

    # Update categories from gsheet
    db.sync_categories()

    # Update 'training' data
    db.update_training_data()

    # Pick nearest neighbors for uncategorized items
    db.predict_categories()

    # Push transactions to Google
    gs.push_gsheet_txns()

    # TODO - deal with income

    # TODO - double-check signs of all outputs

    # TODO - Backfill balances in processed_balances using transactions

    # TODO - push balances and summaries to Google

    # TODO - add printouts / verbose language throughout

    # TODO - address ndarray from ragged nested sequencys warning

if __name__ == '__main__':
    main()