#!/python3

import re
import datetime

import plaid
import json
from plaid.api import plaid_api
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.sandbox_item_reset_login_request import SandboxItemResetLoginRequest
from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest
from plaid.model.item_get_request import ItemGetRequest
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from plaid.model.products import Products
from plaid.model.country_code import CountryCode
from typing import Optional, List


class AccountBalance:
    def __init__(self, data):
        self.raw_data = data.to_dict()
        self.account_id        = data['account_id']
        self.account_name      = data['name']
        self.account_type      = str(data['type'])
        self.account_subtype   = data['subtype']
        self.account_number    = data['mask']
        self.balance_current   = data['balances']['current']
        self.balance_available = data['balances']['available']
        self.balance_limit     = data['balances']['limit']
        self.currency_code     = data['balances']['iso_currency_code']


class AccountInfo:
    def __init__(self, data):
        self.raw_data = data.to_dict()
        self.item_id                   = data['item']['item_id']
        self.institution_id            = data['item']['institution_id']
        self.ts_consent_expiration     = parse_optional_iso8601_timestamp(data['item']['consent_expiration_time'])
        self.ts_last_failed_update     = parse_optional_iso8601_timestamp(data['status']['transactions']['last_failed_update'])
        self.ts_last_successful_update = parse_optional_iso8601_timestamp(data['status']['transactions']['last_successful_update'])


class Transaction:
    def __init__(self, data):
        if type(data) == dict:
            self.raw_data = data
        else:
            self.raw_data = data.to_dict()
        self.account_id     = data['account_id']
        self.date           = data['date']
        self.transaction_id = data['transaction_id']
        self.pending        = data['pending']
        self.merchant_name  = data['merchant_name']
        self.amount         = data['amount']
        self.currency_code  = data['iso_currency_code']

    def __str__(self):
        return "%s %s %s - %4.2f %s" % ( self.date, self.transaction_id, self.merchant_name, self.amount, self.currency_code )


def parse_optional_iso8601_timestamp(ts: Optional[str]) -> datetime.datetime:
    if ts is None:
        return None
    # sometimes the milliseconds coming back from plaid have less than 3 digits
    # which fromisoformat hates - it also hates "Z", so strip those off from this
    # string (the milliseconds hardly matter for this purpose, and I'd rather avoid
    # having to pull dateutil JUST for this parsing)
    print(ts)
    return datetime.datetime.fromisoformat(re.sub(r"[.][0-9]+Z", "+00:00", str(ts)))


def raise_plaid(ex):
    response = json.loads(ex.body)
    code = response['error_code']
    if code == 'NO_ACCOUNTS':
        raise PlaidNoApplicableAccounts(ex)
    elif code == 'ITEM_LOGIN_REQUIRED':
        raise PlaidAccountUpdateNeeded(ex)
    else:
        raise PlaidUnknownError(ex)


def wrap_plaid_error(f):
    def wrap(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except plaid.ApiException as ex:
            raise_plaid(ex)
    return wrap


class PlaidError(Exception):
    def __init__(self, plaid_error):
        super().__init__()
        response = json.loads(plaid_error.body)
        code = response['error_code']
        self.plaid_error = plaid_error
        self.message = code

    def __str__(self):
        return "%s" % (self.message)


class PlaidUnknownError(PlaidError):
    pass


class PlaidNoApplicableAccounts(PlaidError):
    pass


class PlaidAccountUpdateNeeded(PlaidError):
    pass


class PlaidAPI():
    def __init__(self, client_id: str, secret: str, environment: str, suppress_warnings=True):
        hosts = {
            'sandbox': plaid.Environment.Sandbox,
            'development': plaid.Environment.Development
        }

        configuration = plaid.Configuration(
            host=hosts[environment],
            api_key={
                'clientId': client_id,
                'secret': secret,
                'plaidVersion': '2020-09-14'
                }
        )

        api_client = plaid.ApiClient(configuration)
        self.client = plaid_api.PlaidApi(api_client)

    @wrap_plaid_error
    def get_link_update_token(self, access_token=None) -> str:
        """
        Calls the /link/token/create workflow, which returns an access token
        which can be used to initate the account linking process or, if an access_token
        is provided, to update an existing linked account.

        This token is used by the web-browser/JavaScript API to exchange for a public
        token to finalize the linking process.

        https://plaid.com/docs/api/tokens/#token-exchange-flow
        """

        # Create a link_token for the given user
        request = LinkTokenCreateRequest(
                client_name="Plaid Test App",
                country_codes=[CountryCode('US')],
                language='en',
                access_token=access_token,
                redirect_uri='https://127.0.0.1:4583/link.html',
                user=LinkTokenCreateRequestUser(
                    client_user_id='abc_123'
                )
            )
        response = self.client.link_token_create(request)           

        return response['link_token']

    @wrap_plaid_error
    def get_link_token(self, access_token=None) -> str:
        """
        Calls the /link/token/create workflow, which returns an access token
        which can be used to initate the account linking process or, if an access_token
        is provided, to update an existing linked account.

        This token is used by the web-browser/JavaScript API to exchange for a public
        token to finalize the linking process.

        https://plaid.com/docs/api/tokens/#token-exchange-flow
        """

        # Create a link_token for the given user
        request = LinkTokenCreateRequest(
                products=[Products("transactions")],
                client_name="Plaid Test App",
                country_codes=[CountryCode('US')],
                language='en',
                redirect_uri='https://127.0.0.1:4583/link.html',
                user=LinkTokenCreateRequestUser(
                    client_user_id='abc_123'
                )
            )
        response = self.client.link_token_create(request)           

        return response['link_token']

    @wrap_plaid_error
    def exchange_public_token(self, public_token: str) -> str:
        """
        Exchange a temporary public token for a permanent private
        access token.
        """
        request = ItemPublicTokenExchangeRequest(
        public_token=public_token
        )

        return self.client.item_public_token_exchange(request)

    @wrap_plaid_error
    def sandbox_reset_login(self, access_token: str) -> str:
        """
        Only applicable to sandbox environment. Resets the login
        details for a specific account so you can test the update
        account flow. 

        Otherwise, attempting to update will just display "Account
        already connected." in the Plaid browser UI.
        """

        request = SandboxItemResetLoginRequest(access_token=access_token)

        return self.client.sandbox_item_reset_login(request)

    @wrap_plaid_error
    def get_item_info(self, access_token: str)->AccountInfo:
        """
        Returns account information associated with this particular access token.
        """

        request = ItemGetRequest(access_token=access_token)
        response = self.client.item_get(request)

        return AccountInfo(response)

    @wrap_plaid_error
    def get_account_balance(self, access_token:str)->List[AccountBalance]:
        """
        Returns the balances of all accounts associated with this particular access_token.
        """
        request = AccountsBalanceGetRequest(access_token=access_token)
        resp = self.client.accounts_balance_get(request)
        return list( map( AccountBalance, resp['accounts'] ) )

    @wrap_plaid_error
    def get_transactions(self, access_token:str, start_date:datetime.date, end_date:datetime.date, account_ids:Optional[List[str]]=None, status_callback=None):
        ret = []
        total_transactions = None
        while True:
            request = TransactionsGetRequest(
                        access_token=access_token,
                        start_date=start_date,
                        end_date=end_date,
                        options=TransactionsGetRequestOptions(
                            include_personal_finance_category=True
                        )
            )
            response = self.client.transactions_get(request)

            total_transactions = response['total_transactions']

            ret += [
                Transaction(t)
                for t in response['transactions']
            ]

            if status_callback: status_callback(len(ret), total_transactions)
            if len(ret) >= total_transactions: break

        return ret
