#!.env/bin/python

import argparse
import datetime
from datetime import tzinfo
import sys
from collections import namedtuple
from argparse import Namespace

import config
import plaidapi
import transactionsdb
from plaidapi import PlaidAccountUpdateNeeded, PlaidError


# TODO - Refactor o(separate plaid sync from google sync from ml etc.

def parse_options():
    parser = argparse.ArgumentParser(description="Synchronize Plaid transactions and balances to local SQLite3 database")

    def valid_date(value):
        try:
            return datetime.datetime.strptime(value, '%Y-%m-%d').date()
        except:
            parser.error("Cannot parse [%s] as valid YYYY-MM-DD date" % value)

    parser.add_argument("-v", "--verbose",    dest="verbose",        action='store_true',  help="If set, status messages will be output during sync process.")
    parser.add_argument("-c", "--config",     dest="config_file",    required=True,        help="[REQUIRED] Configuration filename", metavar="CONFIG_FILE")
    parser.add_argument("-b", "--balances",   dest="balances",       action='store_true',  help="If true, updated balance information (slow) is loaded. Defaults to false.")
    parser.add_argument("-s", "--start_date", dest="start_date",     type=valid_date,      help="[YYYY-MM-DD] Start date for querying transactions. If ommitted, 30 days ago is used.")
    parser.add_argument("-e", "--end_date",   dest="end_date",       type=valid_date,      help="[YYYY-MM-DD] End date for querying transactions. If ommitted, tomorrow is used.")
    parser.add_argument("--update-account",   dest="update_account",                       help="Specify the name of the account to run the update process for."
                                                                                                "To be used when Plaid returns an error that credetials are out of date for an account.")
    parser.add_argument("--link-account",     dest="link_account",                         help="Run with this option to set up an entirely new account through Plaid.")
    parser.add_argument("--check-txns",       dest="check_txns",     action='store_true',  help="Run with this option to print the head of transactions")
    args = parser.parse_args()

    if not args.start_date:
        args.start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).date()

    if not args.end_date:
        args.end_date = datetime.datetime.now().date()

    if args.end_date < args.start_date:
        parser.error("End date [%s] cannot be before start date [%s]" % ( args.end_date, args.start_date ) )
        sys.exit(1)

    return args


class SyncCounts(
        namedtuple("SyncCounts", [
            "new",
            "new_pending",
            "archived",
            "archived_pending",
            "total_fetched",
            "accounts",
        ])):
    pass


class PlaidSynchronizer:
    def __init__(self, db: transactionsdb.TransactionsDB,
                 plaid: plaidapi.PlaidAPI, account_name: str,
                 access_token: str):
        self.transactions = {}
        self.db           = db
        self.plaid        = plaid
        self.account_name = account_name
        self.access_token = access_token
        self.plaid_error  = None
        self.item_info    = None
        self.counts       = SyncCounts(0,0,0,0,0,0)

    def add_transactions(self, transactions):
        self.transactions.update(
            dict(map(lambda t: (t.transaction_id, t), transactions))
        )

    def count_pending(self, tids):
        return len([tid for tid in tids if self.transactions.get(tid) and self.transactions[tid].pending])

    def sync(self, start_date, end_date, fetch_balances=True, verbose=False):
        try:
            if verbose:
                print("Account: %s" % self.account_name)

            # Fetch balances and save out into balances table
            balances = None
            if fetch_balances:
                if verbose:
                    print("Fetching current balances")
                balances = self.plaid.get_account_balance(self.access_token)

                if verbose:
                    print("Saving current balances")
                for balance in balances:
                    if verbose:
                        print(f"Saving balance for {balance.account_id}")
                    self.db.save_balance(balance)
            
            # Fetch account info and save out into account_info table

            if verbose:
                print("    Fetching transactions from %s to %s" % (start_date, end_date))

            self.add_transactions(self.plaid.get_transactions(
                access_token    = self.access_token,
                start_date      = start_date,
                end_date        = end_date,
                status_callback = (lambda c,t: print("        %d/%d fetched" % ( c, t ) )) if verbose else None
            ) )

            account_ids     = set( t.account_id for t in self.transactions.values() )
            tids_existing   = set( self.db.get_transaction_ids( start_date, end_date, list(account_ids) ) )
            tids_fetched    = set( self.transactions.keys() )
            tids_new        = tids_fetched.difference( tids_existing )
            tids_to_archive = tids_existing.difference( tids_fetched )

            self.add_transactions( self.db.fetch_transactions_by_id(tids_to_archive) )

            self.counts = SyncCounts(
                new              = len(tids_new),
                new_pending      = self.count_pending(tids_new),
                archived         = len(tids_to_archive),
                archived_pending = self.count_pending(tids_to_archive),
                total_fetched    = len(tids_fetched),
                accounts         = len(account_ids),
            )

            if verbose:
                print("    Fetched %d new (%d pending), %d to archive (%d were pending), %d total transactions from %d accounts" % (
                    self.counts.new,
                    self.counts.new_pending,
                    self.counts.archived,
                    self.counts.archived_pending,
                    self.counts.total_fetched,
                    self.counts.accounts
                ))

            if verbose:
                print("    Archiving %d transactions" % (len(tids_to_archive)))

            if len(tids_to_archive) > 0:
                self.db.archive_transactions(list(tids_to_archive))

            if verbose:
                if balances:
                    bal_len = len(balances)
                else:
                    bal_len = 0
                print("    Saving %d balances, %d transactions" % (bal_len, len(tids_new)))

            self.db.save_item_info(self.item_info)

            if balances:
                for balance in balances:
                    self.db.save_balance(self.item_info.item_id, balance)

            for tid in tids_new:
                self.db.save_transaction(self.transactions[tid])

        except plaidapi.PlaidError as ex:
            print('ERROR CAUGHT')
            self.plaid_error = ex
            print(f"Here's ex: {type(ex)}")


def try_get_tqdm():
    try:
        import tqdm
        return tqdm.tqdm
    except: # NOQA E722
        return None


def update_account(cfg: config.Config, plaid: plaidapi.PlaidAPI, account_name: str):
    try:
        print("Starting account update process for [%s]" % account_name)

        if account_name not in cfg.get_enabled_accounts():
            print("Unknown account name [%s]." % account_name, file=sys.stderr)
            print("Configured accounts: ", file=sys.stderr)
            for account in cfg.get_enabled_accounts():
                print("    %s" % account, file=sys.stderr)
            sys.exit(1)

        link_token = plaid.get_link_update_token(
            access_token=cfg.get_account_access_token(account_name)
        )

        import webserver
        plaid_response = webserver.serve(
            env=cfg.environment,
            clientName="plaid-sync",
            pageTitle="Update Account Credentials",
            type="update",
            accountName=account_name,
            token=link_token,
        )

        if 'public_token' not in plaid_response:
            print("No public token returned in the response.")
            print("The update process may not have been successful.")
            print("")
            print("This is OK. You can try syncing to confirm, or")
            print("retry the update process. The account data/link")
            print("is not lost.")
            sys.exit(1)

        public_token = plaid_response['public_token']
        print("")
        print(f"Public token obtained [{public_token}].")
        print("")
        print("There is nothing else to do, the account should sync "
              "properly now with the existing credentials.")

        sys.exit(0)
    except PlaidError as ex:
        print("")
        print("Unhandled exception during account update process.")
        print(ex)


def link_account(cfg: config.Config, plaid: plaidapi.PlaidAPI, account_name: str):
    if account_name in cfg.get_all_config_sections():
        print("Cannot link new account - the account name you selected")
        print("is already defined in your local configuration. Re-run with")
        print("a different name.")
        sys.exit(1)

    link_token = plaid.get_link_token()

    import webserver
    plaid_response = webserver.serve(
        env=cfg.environment,
        clientName="plaid-sync",
        pageTitle="Link New Account",
        type="link",
        accountName=account_name,
        token=link_token,
    )

    if 'public_token' not in plaid_response:
        print("**** WARNING ****")
        print("Plaid Link process did not return a public token to exchange for a permanent token.")
        print("If the process did complete, you may be able to recover the public token from the browser.")
        print("Check the webpage for the public token, and if you see it in the JSON response, re-run this")
        print("command with:")
        print("--link-account '%s' --link-account-token '<TOKEN>" % account_name)
        sys.exit(1)

    public_token = plaid_response['public_token']
    print("")
    print(f"Public token obtained [{public_token}]. "
          "Exchanging for access token.")

    try:
        exchange_response = plaid.exchange_public_token(public_token)
    except PlaidError as ex:
        print("**** WARNING ****")
        print("Error exchanging Plaid public token for access token.")
        print("")
        print(ex)
        print("")
        print("You can attempt the exchange again by re-runnning this command with:")
        print("--link-account '%s' --link-account-token '<TOKEN>" % account_name)
        sys.exit(1)

    access_token = exchange_response['access_token']

    print("Access token received: %s" % access_token)
    print("")

    print("Saving new link to configuration file")
    cfg.add_account(account_name, access_token)

    print("")
    print(f"{account_name} is linked and is ready to sync.")

    sys.exit(0)


def sync_plaid_data(update_account, link_account, cfg, plaid, start_date, end_date, db, fetch_balances, verbose):
    # Update accounts if stated in arguments
    if update_account:
        update_account(cfg, plaid, update_account)
        return

    # Link new accounts if stated in arguments
    if link_account:
        link_account(cfg, plaid, link_account)
        return

    # Check for configured accounts in config
    if not cfg.get_enabled_accounts():
        print("There are no configured Plaid accounts in the specified "
              "configuration file.")
        print("")
        print("Re-run with --link-account to add one.")
        sys.exit(1)

    # Begin account sync process
    results = {}

    def process_account(account_name):
        sync = PlaidSynchronizer(db, plaid, account_name, cfg.get_account_access_token(account_name))
        sync.sync(start_date, end_date, fetch_balances=fetch_balances, verbose=verbose)
        results[account_name] = sync

    tqdm = try_get_tqdm() if not verbose else None
    if tqdm:
        for account_name in tqdm(cfg.get_enabled_accounts(), desc="Synchronizing Plaid accounts", leave=False):
            process_account(account_name)
    else:
        for account_name in cfg.get_enabled_accounts():
            process_account(account_name)

    print("")
    print("")
    print("Finished syncing %d Plaid accounts" % (len(results)))
    print("")
    for account_name, sync in results.items():
        print("%-5s: %2d new transactions (%d pending), %2d archived transactions over %d accounts" % (
            account_name,
            sync.counts.new,
            sync.counts.new_pending,
            sync.counts.archived,
            sync.counts.accounts,
        ))

        if sync.plaid_error:
            import textwrap
            print("%5s: *** Plaid Error ***" % "")
            for i, line in enumerate(textwrap.wrap(str(sync.plaid_error), width=40)):
                print("%5s: %s" % ("", line))
            if type(sync.plaid_error) == plaidapi.PlaidAccountUpdateNeeded:
                print("%5s: *** re-run with: ***" % "")
                print("%5s: --update '%s'" % ("", account_name))
                print("%5s: to fix" % "")

    # check for any out of date accounts
    for account_name, sync in results.items():
        if not sync.item_info:
            continue

        now = datetime.datetime.now(tz=datetime.timezone.utc)

        if sync.item_info.ts_last_failed_update > sync.item_info.ts_last_successful_update:
            print("%-5s: Last attempt failed!  Last failure: %s  Last success: %s" % (
                account_name, sync.item_info.ts_last_failed_update, sync.item_info.ts_last_successful_update
            ))
        elif sync.item_info.ts_last_successful_update < (now - datetime.timedelta(days=3)):
            print("%-5s: Last successful update > 3 days ago!  Last failure: %s  Last success: %s" % (
                account_name, sync.item_info.ts_last_failed_update, sync.item_info.ts_last_successful_update
            ))


def main():
    # args = parse_options()

    # Collect configuration and options
    args = Namespace(balances=True, check_txns=False, config_file='config/budget_config', 
                     end_date=datetime.date(2023, 8, 6), link_account=None, start_date=datetime.date(2023, 7, 7), 
                     update_account=None, verbose=False)
    cfg = config.Config(args.config_file)
    db = transactionsdb.TransactionsDB(cfg.get_dbfile())
    plaid = plaidapi.PlaidAPI(**cfg.get_plaid_client_config())

    # Sync plaid data
    sync_plaid_data(args.update_account, args.link_account, cfg, plaid, args.start_date, args.end_date, db,
                    args.balances, args.verbose)

    # TODO - add google steps from other code
    

    

if __name__ == '__main__':
    main()