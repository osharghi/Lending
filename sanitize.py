#!/usr/bin/env python
#
# Copyright (c) 2015 Omid Sharghi

"""Lending Club historical data sanitization module.

This module reads the rows of the Lending Club historical data file and
validates data before writing the rows using stdout.

"""

import argparse
import csv
import datetime
import sys


def sanitize(input_file, output_file):
    """Reads the provided input file and writes sanitized rows to stdout.

    The LendingClub historical data CSV file contains data that needs to be
    formatted in order to be utilized.  Specific rows have been targeted to
    be formatted while other rows are directly written without being altered.
    Rows that cannot be formatted are skipped instead of being written.

    Args:
        input_file: File path of LendingClub historical data CSV.
    """
    with open(input_file) as csvfile:
        next(csvfile)  # Skip first line which isn't the header
        reader = csv.DictReader(csvfile)
        with open(output_file, 'w') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                if not row['id'].isdigit():
                    break
                try:
                    row['loan_amnt'] = _divisible_by_25(row['loan_amnt'])
                    row['funded_amnt'] = _divisible_by_25(row['funded_amnt'])
                    row['funded_amnt_inv'] = _divisible_by_25(row['funded_amnt_inv'])
                    row['term'] = _term_to_int(row['term'])
                    row['int_rate'] = _percent_to_float(row['int_rate'])
                    row['issue_d'] = _date_to_iso(row['issue_d'])
                    row['earliest_cr_line'] = _date_to_iso(row['earliest_cr_line'])
                    row['last_pymnt_d'] = _date_to_iso(row['last_pymnt_d'])
                    row['next_pymnt_d'] = _date_to_iso(row['next_pymnt_d'])
                    row['last_credit_pull_d'] = _date_to_iso(row['last_credit_pull_d'])
                    row['revol_util'] = _percent_to_float(row['revol_util'])
                    writer.writerow(row)
                except ValueError:
                    continue


def _divisible_by_25(value):
    float_value = float(value)
    if float_value % 25 == 0 and float_value != 0:
        return float_value
    else:
        raise ValueError


def _percent_to_float(string_percentage):
    return float(string_percentage.strip('%')) / 100


def _date_to_iso(csv_date):
    if csv_date != '':
        date = datetime.datetime.strptime(csv_date, '%b-%Y')
        return date.strftime('%Y-%m-%d')
    else:
        return csv_date


def _term_to_int(term_string):
    return int(term_string.strip(' months'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv1", help="Name of input CSV")
    parser.add_argument("csv2", help="Name of output CSV")
    args = parser.parse_args()
    sanitize(args.csv1, args.csv2)


if __name__ == "__main__":
    main()
