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


# TODO - Refactor (separate plaid sync from google sync from ml etc)
# TODO - update all accounts

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
    parser.add_argument("--update-account",   dest="account_update",                       help="Specify the name of the account to run the update process for."
                                                                                                "To be used when Plaid returns an error that credetials are out of date for an account.")
    parser.add_argument("--link-account",     dest="account_link",                         help="Run with this option to set up an entirely new account through Plaid.")
    parser.add_argument("--check-txns",       dest="check_txns",     action='store_true',  help="Run with this option to print the head of transactions")
    parser.add_argument("--manual-path-txn",    dest="manual_txn",                         help="Populate this to direct to a CSV for raw_transaction upload")
    parser.add_argument("--manual-path-acct",    dest="manual_acct",                       help="Populate this to direct to a CSV for account_info upload")

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
            "total_fetched",
            "accounts",
        ])):
    pass


class PlaidSynchronizer:
    def __init__(self, db: transactionsdb.TransactionsDB,
                 papi: plaidapi.PlaidAPI, account_name: str,
                 access_token: str):
        self.transactions = {}
        self.db           = db
        self.papi         = papi
        self.account_name = account_name
        self.access_token = access_token
        self.plaid_error  = None
        self.account_info = None
        self.counts       = SyncCounts(0,0,0,0)

    def add_transactions(self, transactions):
        self.transactions.update(
            dict(map(lambda t: (t.txn_id, t), transactions))
        )

    def count_pending(self, tids):
        return len([tid for tid in tids if self.transactions.get(tid) and self.transactions[tid].pending])

    def sync(self, start_date, end_date, fetch_balances=True, verbose=True):
        try:
            if verbose:
                print("\n\nAccount: %s" % self.account_name)

            # Fetch balances and save out into raw_balances parquet
            balances = None
            if fetch_balances:
                if verbose:
                    print("    Fetching current balances")
                balances = self.papi.get_account_balance(self.access_token)

                if verbose:
                    print("    Saving current balances")
                self.db.save_balances(balances)
            
            # Fetch account info and save out into account_info parquet
            if verbose:
                print("    Fetching account info")
            account_info = self.papi.get_account_info(self.access_token, self.account_name)

            self.db.save_account_info(account_info)

            # Fetch transactions, archive as needed and save out into raw_transactions parquet
            if verbose:
                print("    Fetching transactions from %s to %s" % (start_date, end_date))

            self.add_transactions(self.papi.get_transactions(
                access_token    = self.access_token,
                start_date      = start_date,
                end_date        = end_date,
                status_callback = (lambda c,t: print("        %d/%d fetched" % ( c, t ) )) if verbose else None
            ) )

            account_ids     = set( t.account_id for t in self.transactions.values() )
            tids_existing   = set( self.db.get_transaction_ids( start_date, end_date, list(account_ids) ) )
            tids_fetched    = set( self.transactions.keys() )
            tids_new        = tids_fetched.difference( tids_existing )

            self.counts = SyncCounts(
                new              = len(tids_new),
                new_pending      = self.count_pending(tids_new),
                total_fetched    = len(tids_fetched),
                accounts         = len(account_ids),
            )

            if verbose:
                print("    Fetched %d new (%d pending), %d total transactions from %d accounts" % (
                    self.counts.new,
                    self.counts.new_pending,
                    self.counts.total_fetched,
                    self.counts.accounts
                ))

            self.db.save_transactions([self.transactions.get(tid) for tid in tids_new])

        except plaidapi.PlaidError as ex:
            self.plaid_error = ex


def try_get_tqdm():
    try:
        import tqdm
        return tqdm.tqdm
    except: # NOQA E722
        return None


def update_account(cfg: config.Config, papi: plaidapi.PlaidAPI, account_name: str):
    try:
        print("Starting account update process for [%s]" % account_name)

        if account_name not in cfg.get_enabled_accounts():
            print("Unknown account name [%s]." % account_name, file=sys.stderr)
            print("Configured accounts: ", file=sys.stderr)
            for account in cfg.get_enabled_accounts():
                print("    %s" % account, file=sys.stderr)
            sys.exit(1)

        link_token = papi.get_link_update_token(
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


def link_account(cfg: config.Config, papi: plaidapi.PlaidAPI, account_name: str):
    if account_name in cfg.get_all_config_sections():
        print("Cannot link new account - the account name you selected")
        print("is already defined in your local configuration. Re-run with")
        print("a different name.")
        sys.exit(1)

    link_token = papi.get_link_token()

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
        exchange_response = papi.exchange_public_token(public_token)
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


def sync_plaid_data(account_update, account_link, cfg, papi, start_date, end_date, db, fetch_balances, verbose):
    # Update accounts if stated in arguments
    if account_update:
        update_account(cfg, papi, account_update)
        return

    # Link new accounts if stated in arguments
    if account_link:
        link_account(cfg, papi, account_link)
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
        sync = PlaidSynchronizer(db, papi, account_name, cfg.get_account_access_token(account_name))
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
        print("%-5s: %2d new transactions (%d pending) transactions over %d accounts" % (
            account_name,
            sync.counts.new,
            sync.counts.new_pending,
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