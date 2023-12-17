from plaidsync import *
import os
# from gsheet import *

def main():
    # args = parse_options()

    default_config = 'G:\\My Drive\\01-Banking_Ins\\Budget_Data\\config\\budget_config'

    # Collect configuration and options
    args = Namespace(balances=True, check_txns=False, config_file='config/budget_config', 
                     end_date=datetime.date(2023,11,19), account_link=None, start_date=datetime.date(2023, 1, 1), 
                     account_update=None, verbose=True)
    if os.path.exists(args.config_file):
        config_file = args.config_file
    else:
        config_file = default_config
    cfg = config.Config(config_file)
    dbfile_win, dbfile_mac = cfg.get_dbfiles()
    if os.path.exists(dbfile_win):
        db = transactionsdb.TransactionsDB(dbfile_win)
    else:
        db = transactionsdb.TransactionsDB(dbfile_mac)
    papi = plaidapi.PlaidAPI(**cfg.get_plaid_client_config())

    # Sync plaid data
    sync_plaid_data(args.account_update, args.account_link, cfg, papi, args.start_date, args.end_date, db,
                    args.balances, args.verbose)
    
    # TODO - Sync gsheet data
        # Back up and pull down gsheet data
        # Update processed transaction categories from gsheet
        # Add new transactions to gsheet & update
        # Backup balanaces and add new balances to processed balance dataframe & update

if __name__ == '__main__':
    main()