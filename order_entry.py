# Copyright (c) 2015 Omid Sharghi - All Rights Reserved

"""Lending Club Order Entry module.

Provides the API for clients to submit orders to purchase loans.

"""

from operator import ior

import psycopg2

from lendingclub import LCOrder
from lendingclub.types import StringEnum
from risk import RiskManager


class ExecutionStatus(StringEnum):
    ORDER_FULFILLED = 1
    LOAN_AMNT_EXCEEDED = 2
    NOT_AN_INFUNDING_LOAN = 4
    REQUESTED_AMNT_LOW = 8
    REQUESTED_AMNT_ROUNDED = 16
    AUGMENTED_BY_MERGE = 32
    ELIM_BY_MERGE = 64
    INSUFFICIENT_CASH = 128
    NOT_AN_INVESTOR = 256
    NOT_A_VALID_INVESTMENT = 512
    NOTE_ADDED_TO_PORTFOLIO = 1024
    NOT_A_VALID_PORTFOLIO = 2048
    ERROR_ADDING_NOTE_TO_PORTFOLIO = 4096
    SYSTEM_BUSY = 8192
    UNKNOWN_ERROR = 16384


class Order(object):
    """Creates Order object.

    Creates Order object when instantiated with loan_id and amount.

    Attributes:
        loan_id: A int value representing the loan id
        amount: A float value to represent the amount to invest in a loan
    """
    def __init__(self, loan_id, amount, portfolio_id=None):
        if amount <= 0:
            raise ValueError("Amount must be greater than 0")
        self.loan_id = loan_id
        self.amount = amount
        self.portfolio_id = portfolio_id


class OESession(object):
    def __init__(self, client, host, port, password, user):
        self.client = client
        self.conn = psycopg2.connect(dbname='bank',
                                     user=user,
                                     password=password,
                                     port=port,
                                     host=host)

    def submit_order(self, investor_id, client_id, order):
        """Submits a single order

        Enables user to submit a single order.

        Args:
            investor_id: the investor ID
            client_id: the ID of the client being used
            order: an Order object
        """
        orders = [order]
        self.submit_orders(investor_id, client_id, orders)

    def submit_orders(self, investor_id, client_id, orders):
        """Submits a list of orders

        Enables user to send multiple orders.  The Risk
        Manager is used to screen all orders before being
        submitted.  Each order has to pass risk check to
        be submitted. Orders that fail risk check are
        returned.

        Args:
            investor_id: the investor ID
            client_id: the ID of the client being used
            orders: a list of Order objects
        """
        order_pack = []
        risk_manager = RiskManager.instance()
        for order in orders:
            lc_order = LCOrder(loanId=order.loan_id, requestedAmount=order.amount,
                               portfolioId=order.portfolio_id)
            order_pack.append(lc_order)
            if not risk_manager.check(order):
                return
        self.preliminary_log_order(client_id, order_pack)
        lc_submit_order_ack = self.client.submit_order(investor_id, order_pack)
        self.final_log_order(lc_submit_order_ack)

    def preliminary_log_order(self, client_id, order_pack):
        with self.conn.cursor() as cur:
            for lc_order in order_pack:
                cur.execute('INSERT INTO orders (loan_id, client_id, requested_amount) '
                            'VALUES (%s, %s, %s) RETURNING cl_order_id;',
                            (lc_order.loanId, client_id, lc_order.requestedAmount))
            self.conn.commit()

    def final_log_order(self, lc_submit_order_ack):
        order_list = []
        for lc_order_confirmation in lc_submit_order_ack.orderConfirmations:
            execution_list = lc_order_confirmation.executionStatus
            encoded_executions = get_execution_code(execution_list)
            order_dict = {'loan_id': lc_order_confirmation.loanId,
                          'requested_amount': lc_order_confirmation.requestedAmount,
                          'invested_amount': lc_order_confirmation.investedAmount,
                          'execution_code': encoded_executions,
                          'instruct_id': lc_submit_order_ack.orderInstructId}
            order_list.append(order_dict)
        with self.conn.cursor() as cur:
            cur.executemany("""UPDATE orders SET (invested_amount, ex_order_id,
                            time_acknowledged, execution_code) =
                            (%(invested_amount)s, %(instruct_id)s, current_timestamp,
                            %(execution_code)s) WHERE loan_id = %(loan_id)s and
                            requested_amount = %(requested_amount)s and execution_code IS NULL""",
                            order_list)
            self.conn.commit()


def get_execution_code(execution_list):
    encoded_execution_list = [ExecutionStatus.from_str(m).value for m in execution_list]
    return reduce(ior, encoded_execution_list)




