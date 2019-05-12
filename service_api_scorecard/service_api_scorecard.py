import ast
import datetime
import tempfile

import pymongo

from functions import temporary_file_util
from model.db_init import db_init
import pandas as pd

from service_api_scorecard.utilities.woe import woe_conversion, woe_graph, woe_analysis, mono_bin, char_bin
from service_api_scorecard.utilities.scorecard import scorecard

import pandas as pd

import pandas as pd
from dateutil.parser import parse

all_cols = ['last_account_opened_months_6',
            'age',
            'last_loan_drawn_in_months_7',
            'current_bal_for_active_accounts_13',
            'no_acs_overdue_total_accounts_ratio_2',
            'latest_cibil_score',
            'total_value_of_accounts_52',
            'current_bal_for_active_accounts_6',
            'no_of_running_bl_pl_1',
            'current_bal_for_active_accounts_52',
            'last_account_opened_months_10',
            'last_loan_drawn_in_months_61',
            'monthly_emi_for_active_accounts_6',
            'last_account_opened_months_52',
            'no_acs_overdue_total_accounts_ratio_6',
            'last_account_opened_months_13',
            'total_value_of_accounts_51',
            'no_acs_overdue_total_accounts_ratio_13',
            'sanctioned_amt_for_active_accounts_51',
            'amt_bl_paid_off_13',
            'last_account_opened_months_0']

cibil_cols_required = ['last_account_opened_months_6',
                       'last_loan_drawn_in_months_7',
                       'current_bal_for_active_accounts_13',
                       'no_acs_overdue_total_accounts_ratio_2',
                       'latest_cibil_score',
                       'total_value_of_accounts_52',
                       'current_bal_for_active_accounts_6',
                       'no_of_running_bl_pl_1',
                       'current_bal_for_active_accounts_52',
                       'last_account_opened_months_10',
                       'last_loan_drawn_in_months_61',
                       'monthly_emi_for_active_accounts_6',
                       'last_account_opened_months_52',
                       'no_acs_overdue_total_accounts_ratio_6',
                       'last_account_opened_months_13',
                       'total_value_of_accounts_51',
                       'no_acs_overdue_total_accounts_ratio_13',
                       'sanctioned_amt_for_active_accounts_51',
                       'amt_bl_paid_off_13',
                       'last_account_opened_months_0']

data_df = pd.DataFrame(columns=all_cols)


def cibil_flat_data_func(business_id):
    db = db_init()
    cas_bus_partner_cibil_analysis = db['cas_bus_partner_cibil_analysis']
    data = cas_bus_partner_cibil_analysis.find({"business_id": business_id}).sort("_id", pymongo.DESCENDING).limit(1)

    df = pd.DataFrame(list(data))

    print(df.columns)
    # print(df.dtypes)

    # print(df['Account'])

    df3 = df.drop_duplicates('business_id')

    accounts_column_names = ["business_id",
                             "cibil_pull_date",
                             "last_account_opened_months",
                             "monthly_emi_for_active_accounts",
                             "sanctioned_amt_for_active_accounts",
                             "no_of_loans_opened_last_3_months",
                             "value_of_loans_opened_last_12_months",
                             "total_number_of_accounts",
                             "no_of_loans_paid_off_successfully",
                             "value_of_loans_opened_last_3_months",
                             "number_of_active_accounts",
                             "total_value_of_accounts",
                             "account_type",
                             "no_of_loans_opened_last_12_months",
                             "latest_disbursed_date",
                             "value_of_loans_paid_off_successfully",
                             "current_bal_for_active_accounts",

                             "amt_written_off_total_liabilities_ratio",
                             "no_acs_written_off_total_liabilities_ratio",
                             "amt_bl_paid_off",
                             "last_loan_drawn_in_months",
                             "no_acs_overdue_total_accounts_ratio",
                             "no_of_bl_paid_off",
                             "credit_card_usage_total_limits_ratio",
                             "no_of_running_bl_pl",
                             "no_of_enquiries_last_3_months",
                             "no_of_enquiries_last_12_months"
                             ]

    cols_scoring = ["amt_written_off_total_liabilities_ratio",
                    "no_acs_written_off_total_liabilities_ratio",
                    "amt_bl_paid_off",
                    "last_loan_drawn_in_months",
                    "no_acs_overdue_total_accounts_ratio",
                    "no_of_bl_paid_off",
                    "credit_card_usage_total_limits_ratio",
                    "no_of_running_bl_pl"]

    enquiry = ["no_of_enquiries_last_3_months",
               "no_of_enquiries_last_12_months"]

    df_mongo_buckets = pd.DataFrame(columns=accounts_column_names)

    i = 0
    print(df3.dtypes)

    print(df3)

    # df3['business_id'] = df3['business_id'].astype(str)
    print(df3.dtypes)

    for row in df3.iterrows():
        account = row[1]['Account']

        id = row[1]['_id']
        if 'cibil_pull_date' in row[1]:
            cibil_pull_date = row[1]['cibil_pull_date']
            if not str(cibil_pull_date) == "nan":
                if "$date" in cibil_pull_date:
                    cibil_pull_date = cibil_pull_date['$date']

        if 'business_id' in row[1]:
            business_id = str(row[1]['business_id'])
        else:
            business_id = str(0)

        print(business_id)

        if 'business_pan' in row[1]:
            business_pan = row[1]['business_pan']
        else:
            business_pan = 0

        for data in account:
            # print(data)

            ################################

            print(row[1])

            df_mongo_buckets.loc[i, 'business_id'] = str(business_id)
            # df_mongo_buckets.loc[i, 'business_pan'] = business_pan
            df_mongo_buckets.loc[i, 'cibil_pull_date'] = cibil_pull_date

            # print(business_id)
            # print(business_pan)
            # print(id)
            # print(type(business_pan))
            # print(type(row[1]['Scoring']))

            scoring = str(row[1]['Scoring'])
            # print(type(scoring))
            if scoring != "nan":
                # print(scoring)

                # scoring = ast.literal_eval(scoring)
                if 'Scoring' in row[1]:
                    # print(row[1])
                    scoring = row[1]['Scoring']
                    if "amt_written_off_total_liabilities_ratio" in scoring:
                        df_mongo_buckets.loc[i, 'amt_written_off_total_liabilities_ratio'] = scoring[
                            'amt_written_off_total_liabilities_ratio']

                    if "no_acs_written_off_total_liabilities_ratio" in scoring:
                        df_mongo_buckets.loc[i, 'no_acs_written_off_total_liabilities_ratio'] = scoring[
                            'no_acs_written_off_total_liabilities_ratio']

                    if "amt_bl_paid_off" in scoring:
                        df_mongo_buckets.loc[i, 'amt_bl_paid_off'] = scoring[
                            'amt_bl_paid_off']

                    if "last_loan_drawn_in_months" in scoring:
                        df_mongo_buckets.loc[i, 'last_loan_drawn_in_months'] = scoring[
                            'last_loan_drawn_in_months']

                    if "no_acs_overdue_total_accounts_ratio" in scoring:
                        df_mongo_buckets.loc[i, 'no_acs_overdue_total_accounts_ratio'] = scoring[
                            'no_acs_overdue_total_accounts_ratio']

                    if "no_of_bl_paid_off" in scoring:
                        df_mongo_buckets.loc[i, 'no_of_bl_paid_off'] = scoring[
                            'no_of_bl_paid_off']

                    if "credit_card_usage_total_limits_ratio" in scoring:
                        df_mongo_buckets.loc[i, 'credit_card_usage_total_limits_ratio'] = scoring[
                            'credit_card_usage_total_limits_ratio']

                    if "no_of_running_bl_pl" in scoring:
                        df_mongo_buckets.loc[i, 'no_of_running_bl_pl'] = scoring[
                            'no_of_running_bl_pl']

            ###################
            enquiry = str(row[1]['Enquiry'])

            if enquiry != "nan":
                if 'Enquiry' in row[1]:
                    enquiry = ast.literal_eval(enquiry)

                    if "no_of_enquiries_last_3_months" in enquiry:
                        df_mongo_buckets.loc[i, 'no_of_enquiries_last_3_months'] = enquiry[
                            'no_of_enquiries_last_3_months']

                    if "no_of_enquiries_last_12_months" in enquiry:
                        df_mongo_buckets.loc[i, 'no_of_enquiries_last_12_months'] = enquiry[
                            'no_of_enquiries_last_12_months']

            #################################
            if "last_account_opened_months" in data:
                df_mongo_buckets.loc[i, 'last_account_opened_months'] = data['last_account_opened_months']

            if "monthly_emi_for_active_accounts" in data:
                df_mongo_buckets.loc[i, 'monthly_emi_for_active_accounts'] = data['monthly_emi_for_active_accounts']

            if "sanctioned_amt_for_active_accounts" in data:
                df_mongo_buckets.loc[i, 'sanctioned_amt_for_active_accounts'] = data[
                    'sanctioned_amt_for_active_accounts']

            if "no_of_loans_opened_last_3_months" in data:
                df_mongo_buckets.loc[i, 'no_of_loans_opened_last_3_months'] = data['no_of_loans_opened_last_3_months']

            if "total_number_of_accounts" in data:
                df_mongo_buckets.loc[i, 'total_number_of_accounts'] = data['total_number_of_accounts']

            if "no_of_loans_paid_off_successfully" in data:
                df_mongo_buckets.loc[i, 'no_of_loans_paid_off_successfully'] = data['no_of_loans_paid_off_successfully']

            if "value_of_loans_opened_last_3_months" in data:
                df_mongo_buckets.loc[i, 'value_of_loans_opened_last_3_months'] = data[
                    'value_of_loans_opened_last_3_months']

            if "value_of_loans_opened_last_12_months" in data:
                df_mongo_buckets.loc[i, 'value_of_loans_opened_last_12_months'] = data[
                    'value_of_loans_opened_last_12_months']

            if "number_of_active_accounts" in data:
                df_mongo_buckets.loc[i, 'number_of_active_accounts'] = data['number_of_active_accounts']

            if "total_value_of_accounts" in data:
                df_mongo_buckets.loc[i, 'total_value_of_accounts'] = data['total_value_of_accounts']

            if "total_number_of_accounts" in data:
                df_mongo_buckets.loc[i, 'total_number_of_accounts'] = data['total_number_of_accounts']

            if "no_of_loans_opened_last_12_months" in data:
                df_mongo_buckets.loc[i, 'no_of_loans_opened_last_12_months'] = data['no_of_loans_opened_last_12_months']

            if "account_type" in data:
                df_mongo_buckets.loc[i, 'account_type'] = data['account_type']

            if "latest_disbursed_date" in data:
                df_mongo_buckets.loc[i, 'latest_disbursed_date'] = data['latest_disbursed_date']

            if "value_of_loans_paid_off_successfully" in data:
                df_mongo_buckets.loc[i, 'value_of_loans_paid_off_successfully'] = data[
                    'value_of_loans_paid_off_successfully']

            if "current_bal_for_active_accounts" in data:
                df_mongo_buckets.loc[i, 'current_bal_for_active_accounts'] = data['current_bal_for_active_accounts']

            i += 1

    # df_mongo_buckets.to_csv(
    #     'api_cibil_group_data3.csv')
    #
    # df = pd.read_csv(
    #     'api_cibil_group_data3.csv')

    # all columns
    all_cols = ['last_account_opened_months',
                'monthly_emi_for_active_accounts', 'sanctioned_amt_for_active_accounts',
                'no_of_loans_opened_last_3_months',
                'value_of_loans_opened_last_12_months', 'total_number_of_accounts',
                'no_of_loans_paid_off_successfully',
                'value_of_loans_opened_last_3_months', 'number_of_active_accounts',
                'total_value_of_accounts',
                'no_of_loans_opened_last_12_months', 'latest_disbursed_date',
                'value_of_loans_paid_off_successfully',
                'current_bal_for_active_accounts',
                'amt_written_off_total_liabilities_ratio',
                'no_acs_written_off_total_liabilities_ratio', 'amt_bl_paid_off',
                'last_loan_drawn_in_months', 'no_acs_overdue_total_accounts_ratio',
                'no_of_bl_paid_off', 'credit_card_usage_total_limits_ratio',
                'no_of_running_bl_pl', 'no_of_enquiries_last_3_months',
                'no_of_enquiries_last_12_months']

    unique_account_type = ['5', '51', '10', '13', '2', '0', '6', '7', '9', '1', '17', '61', '52', '39',
                           '34', '32', '3', '53', '12', '4', '8', '59', '15', '33', '54', '40', '55', '36',
                           '14', '50', '43', 'None', 'blank', '41']

    generate_all_cols = []
    for rows in unique_account_type:
        for col in all_cols:
            generate_all_cols.append(str(col) + "_" + str(rows))

    print(generate_all_cols)

    df_cols = ['last_account_opened_months_5', 'monthly_emi_for_active_accounts_5',
               'sanctioned_amt_for_active_accounts_5',
               'no_of_loans_opened_last_3_months_5', 'value_of_loans_opened_last_12_months_5',
               'total_number_of_accounts_5',
               'no_of_loans_paid_off_successfully_5', 'value_of_loans_opened_last_3_months_5',
               'number_of_active_accounts_5', 'total_value_of_accounts_5', 'no_of_loans_opened_last_12_months_5',
               'latest_disbursed_date_5', 'value_of_loans_paid_off_successfully_5', 'current_bal_for_active_accounts_5',
               'amt_written_off_total_liabilities_ratio_5', 'no_acs_written_off_total_liabilities_ratio_5',
               'amt_bl_paid_off_5', 'last_loan_drawn_in_months_5', 'no_acs_overdue_total_accounts_ratio_5',
               'no_of_bl_paid_off_5', 'credit_card_usage_total_limits_ratio_5', 'no_of_running_bl_pl_5',
               'no_of_enquiries_last_3_months_5', 'no_of_enquiries_last_12_months_5', 'last_account_opened_months_51',
               'monthly_emi_for_active_accounts_51', 'sanctioned_amt_for_active_accounts_51',
               'no_of_loans_opened_last_3_months_51', 'value_of_loans_opened_last_12_months_51',
               'total_number_of_accounts_51', 'no_of_loans_paid_off_successfully_51',
               'value_of_loans_opened_last_3_months_51', 'number_of_active_accounts_51', 'total_value_of_accounts_51',
               'no_of_loans_opened_last_12_months_51', 'latest_disbursed_date_51',
               'value_of_loans_paid_off_successfully_51', 'current_bal_for_active_accounts_51',
               'amt_written_off_total_liabilities_ratio_51', 'no_acs_written_off_total_liabilities_ratio_51',
               'amt_bl_paid_off_51', 'last_loan_drawn_in_months_51', 'no_acs_overdue_total_accounts_ratio_51',
               'no_of_bl_paid_off_51', 'credit_card_usage_total_limits_ratio_51', 'no_of_running_bl_pl_51',
               'no_of_enquiries_last_3_months_51', 'no_of_enquiries_last_12_months_51', 'last_account_opened_months_10',
               'monthly_emi_for_active_accounts_10', 'sanctioned_amt_for_active_accounts_10',
               'no_of_loans_opened_last_3_months_10', 'value_of_loans_opened_last_12_months_10',
               'total_number_of_accounts_10', 'no_of_loans_paid_off_successfully_10',
               'value_of_loans_opened_last_3_months_10', 'number_of_active_accounts_10', 'total_value_of_accounts_10',
               'no_of_loans_opened_last_12_months_10', 'latest_disbursed_date_10',
               'value_of_loans_paid_off_successfully_10', 'current_bal_for_active_accounts_10',
               'amt_written_off_total_liabilities_ratio_10', 'no_acs_written_off_total_liabilities_ratio_10',
               'amt_bl_paid_off_10', 'last_loan_drawn_in_months_10', 'no_acs_overdue_total_accounts_ratio_10',
               'no_of_bl_paid_off_10', 'credit_card_usage_total_limits_ratio_10', 'no_of_running_bl_pl_10',
               'no_of_enquiries_last_3_months_10', 'no_of_enquiries_last_12_months_10', 'last_account_opened_months_13',
               'monthly_emi_for_active_accounts_13', 'sanctioned_amt_for_active_accounts_13',
               'no_of_loans_opened_last_3_months_13', 'value_of_loans_opened_last_12_months_13',
               'total_number_of_accounts_13', 'no_of_loans_paid_off_successfully_13',
               'value_of_loans_opened_last_3_months_13', 'number_of_active_accounts_13', 'total_value_of_accounts_13',
               'no_of_loans_opened_last_12_months_13', 'latest_disbursed_date_13',
               'value_of_loans_paid_off_successfully_13', 'current_bal_for_active_accounts_13',
               'amt_written_off_total_liabilities_ratio_13', 'no_acs_written_off_total_liabilities_ratio_13',
               'amt_bl_paid_off_13', 'last_loan_drawn_in_months_13', 'no_acs_overdue_total_accounts_ratio_13',
               'no_of_bl_paid_off_13', 'credit_card_usage_total_limits_ratio_13', 'no_of_running_bl_pl_13',
               'no_of_enquiries_last_3_months_13', 'no_of_enquiries_last_12_months_13', 'last_account_opened_months_2',
               'monthly_emi_for_active_accounts_2', 'sanctioned_amt_for_active_accounts_2',
               'no_of_loans_opened_last_3_months_2', 'value_of_loans_opened_last_12_months_2',
               'total_number_of_accounts_2',
               'no_of_loans_paid_off_successfully_2', 'value_of_loans_opened_last_3_months_2',
               'number_of_active_accounts_2', 'total_value_of_accounts_2', 'no_of_loans_opened_last_12_months_2',
               'latest_disbursed_date_2', 'value_of_loans_paid_off_successfully_2', 'current_bal_for_active_accounts_2',
               'amt_written_off_total_liabilities_ratio_2', 'no_acs_written_off_total_liabilities_ratio_2',
               'amt_bl_paid_off_2', 'last_loan_drawn_in_months_2', 'no_acs_overdue_total_accounts_ratio_2',
               'no_of_bl_paid_off_2', 'credit_card_usage_total_limits_ratio_2', 'no_of_running_bl_pl_2',
               'no_of_enquiries_last_3_months_2', 'no_of_enquiries_last_12_months_2', 'last_account_opened_months_0',
               'monthly_emi_for_active_accounts_0', 'sanctioned_amt_for_active_accounts_0',
               'no_of_loans_opened_last_3_months_0', 'value_of_loans_opened_last_12_months_0',
               'total_number_of_accounts_0',
               'no_of_loans_paid_off_successfully_0', 'value_of_loans_opened_last_3_months_0',
               'number_of_active_accounts_0', 'total_value_of_accounts_0', 'no_of_loans_opened_last_12_months_0',
               'latest_disbursed_date_0', 'value_of_loans_paid_off_successfully_0', 'current_bal_for_active_accounts_0',
               'amt_written_off_total_liabilities_ratio_0', 'no_acs_written_off_total_liabilities_ratio_0',
               'amt_bl_paid_off_0', 'last_loan_drawn_in_months_0', 'no_acs_overdue_total_accounts_ratio_0',
               'no_of_bl_paid_off_0', 'credit_card_usage_total_limits_ratio_0', 'no_of_running_bl_pl_0',
               'no_of_enquiries_last_3_months_0', 'no_of_enquiries_last_12_months_0', 'last_account_opened_months_6',
               'monthly_emi_for_active_accounts_6', 'sanctioned_amt_for_active_accounts_6',
               'no_of_loans_opened_last_3_months_6', 'value_of_loans_opened_last_12_months_6',
               'total_number_of_accounts_6',
               'no_of_loans_paid_off_successfully_6', 'value_of_loans_opened_last_3_months_6',
               'number_of_active_accounts_6', 'total_value_of_accounts_6', 'no_of_loans_opened_last_12_months_6',
               'latest_disbursed_date_6', 'value_of_loans_paid_off_successfully_6', 'current_bal_for_active_accounts_6',
               'amt_written_off_total_liabilities_ratio_6', 'no_acs_written_off_total_liabilities_ratio_6',
               'amt_bl_paid_off_6', 'last_loan_drawn_in_months_6', 'no_acs_overdue_total_accounts_ratio_6',
               'no_of_bl_paid_off_6', 'credit_card_usage_total_limits_ratio_6', 'no_of_running_bl_pl_6',
               'no_of_enquiries_last_3_months_6', 'no_of_enquiries_last_12_months_6', 'last_account_opened_months_7',
               'monthly_emi_for_active_accounts_7', 'sanctioned_amt_for_active_accounts_7',
               'no_of_loans_opened_last_3_months_7', 'value_of_loans_opened_last_12_months_7',
               'total_number_of_accounts_7',
               'no_of_loans_paid_off_successfully_7', 'value_of_loans_opened_last_3_months_7',
               'number_of_active_accounts_7', 'total_value_of_accounts_7', 'no_of_loans_opened_last_12_months_7',
               'latest_disbursed_date_7', 'value_of_loans_paid_off_successfully_7', 'current_bal_for_active_accounts_7',
               'amt_written_off_total_liabilities_ratio_7', 'no_acs_written_off_total_liabilities_ratio_7',
               'amt_bl_paid_off_7', 'last_loan_drawn_in_months_7', 'no_acs_overdue_total_accounts_ratio_7',
               'no_of_bl_paid_off_7', 'credit_card_usage_total_limits_ratio_7', 'no_of_running_bl_pl_7',
               'no_of_enquiries_last_3_months_7', 'no_of_enquiries_last_12_months_7', 'last_account_opened_months_9',
               'monthly_emi_for_active_accounts_9', 'sanctioned_amt_for_active_accounts_9',
               'no_of_loans_opened_last_3_months_9', 'value_of_loans_opened_last_12_months_9',
               'total_number_of_accounts_9',
               'no_of_loans_paid_off_successfully_9', 'value_of_loans_opened_last_3_months_9',
               'number_of_active_accounts_9', 'total_value_of_accounts_9', 'no_of_loans_opened_last_12_months_9',
               'latest_disbursed_date_9', 'value_of_loans_paid_off_successfully_9', 'current_bal_for_active_accounts_9',
               'amt_written_off_total_liabilities_ratio_9', 'no_acs_written_off_total_liabilities_ratio_9',
               'amt_bl_paid_off_9', 'last_loan_drawn_in_months_9', 'no_acs_overdue_total_accounts_ratio_9',
               'no_of_bl_paid_off_9', 'credit_card_usage_total_limits_ratio_9', 'no_of_running_bl_pl_9',
               'no_of_enquiries_last_3_months_9', 'no_of_enquiries_last_12_months_9', 'last_account_opened_months_1',
               'monthly_emi_for_active_accounts_1', 'sanctioned_amt_for_active_accounts_1',
               'no_of_loans_opened_last_3_months_1', 'value_of_loans_opened_last_12_months_1',
               'total_number_of_accounts_1',
               'no_of_loans_paid_off_successfully_1', 'value_of_loans_opened_last_3_months_1',
               'number_of_active_accounts_1', 'total_value_of_accounts_1', 'no_of_loans_opened_last_12_months_1',
               'latest_disbursed_date_1', 'value_of_loans_paid_off_successfully_1', 'current_bal_for_active_accounts_1',
               'amt_written_off_total_liabilities_ratio_1', 'no_acs_written_off_total_liabilities_ratio_1',
               'amt_bl_paid_off_1', 'last_loan_drawn_in_months_1', 'no_acs_overdue_total_accounts_ratio_1',
               'no_of_bl_paid_off_1', 'credit_card_usage_total_limits_ratio_1', 'no_of_running_bl_pl_1',
               'no_of_enquiries_last_3_months_1', 'no_of_enquiries_last_12_months_1', 'last_account_opened_months_17',
               'monthly_emi_for_active_accounts_17', 'sanctioned_amt_for_active_accounts_17',
               'no_of_loans_opened_last_3_months_17', 'value_of_loans_opened_last_12_months_17',
               'total_number_of_accounts_17', 'no_of_loans_paid_off_successfully_17',
               'value_of_loans_opened_last_3_months_17', 'number_of_active_accounts_17', 'total_value_of_accounts_17',
               'no_of_loans_opened_last_12_months_17', 'latest_disbursed_date_17',
               'value_of_loans_paid_off_successfully_17', 'current_bal_for_active_accounts_17',
               'amt_written_off_total_liabilities_ratio_17', 'no_acs_written_off_total_liabilities_ratio_17',
               'amt_bl_paid_off_17', 'last_loan_drawn_in_months_17', 'no_acs_overdue_total_accounts_ratio_17',
               'no_of_bl_paid_off_17', 'credit_card_usage_total_limits_ratio_17', 'no_of_running_bl_pl_17',
               'no_of_enquiries_last_3_months_17', 'no_of_enquiries_last_12_months_17', 'last_account_opened_months_61',
               'monthly_emi_for_active_accounts_61', 'sanctioned_amt_for_active_accounts_61',
               'no_of_loans_opened_last_3_months_61', 'value_of_loans_opened_last_12_months_61',
               'total_number_of_accounts_61', 'no_of_loans_paid_off_successfully_61',
               'value_of_loans_opened_last_3_months_61', 'number_of_active_accounts_61', 'total_value_of_accounts_61',
               'no_of_loans_opened_last_12_months_61', 'latest_disbursed_date_61',
               'value_of_loans_paid_off_successfully_61', 'current_bal_for_active_accounts_61',
               'amt_written_off_total_liabilities_ratio_61', 'no_acs_written_off_total_liabilities_ratio_61',
               'amt_bl_paid_off_61', 'last_loan_drawn_in_months_61', 'no_acs_overdue_total_accounts_ratio_61',
               'no_of_bl_paid_off_61', 'credit_card_usage_total_limits_ratio_61', 'no_of_running_bl_pl_61',
               'no_of_enquiries_last_3_months_61', 'no_of_enquiries_last_12_months_61', 'last_account_opened_months_52',
               'monthly_emi_for_active_accounts_52', 'sanctioned_amt_for_active_accounts_52',
               'no_of_loans_opened_last_3_months_52', 'value_of_loans_opened_last_12_months_52',
               'total_number_of_accounts_52', 'no_of_loans_paid_off_successfully_52',
               'value_of_loans_opened_last_3_months_52', 'number_of_active_accounts_52', 'total_value_of_accounts_52',
               'no_of_loans_opened_last_12_months_52', 'latest_disbursed_date_52',
               'value_of_loans_paid_off_successfully_52', 'current_bal_for_active_accounts_52',
               'amt_written_off_total_liabilities_ratio_52', 'no_acs_written_off_total_liabilities_ratio_52',
               'amt_bl_paid_off_52', 'last_loan_drawn_in_months_52', 'no_acs_overdue_total_accounts_ratio_52',
               'no_of_bl_paid_off_52', 'credit_card_usage_total_limits_ratio_52', 'no_of_running_bl_pl_52',
               'no_of_enquiries_last_3_months_52', 'no_of_enquiries_last_12_months_52', 'last_account_opened_months_39',
               'monthly_emi_for_active_accounts_39', 'sanctioned_amt_for_active_accounts_39',
               'no_of_loans_opened_last_3_months_39', 'value_of_loans_opened_last_12_months_39',
               'total_number_of_accounts_39', 'no_of_loans_paid_off_successfully_39',
               'value_of_loans_opened_last_3_months_39', 'number_of_active_accounts_39', 'total_value_of_accounts_39',
               'no_of_loans_opened_last_12_months_39', 'latest_disbursed_date_39',
               'value_of_loans_paid_off_successfully_39', 'current_bal_for_active_accounts_39',
               'amt_written_off_total_liabilities_ratio_39', 'no_acs_written_off_total_liabilities_ratio_39',
               'amt_bl_paid_off_39', 'last_loan_drawn_in_months_39', 'no_acs_overdue_total_accounts_ratio_39',
               'no_of_bl_paid_off_39', 'credit_card_usage_total_limits_ratio_39', 'no_of_running_bl_pl_39',
               'no_of_enquiries_last_3_months_39', 'no_of_enquiries_last_12_months_39', 'last_account_opened_months_34',
               'monthly_emi_for_active_accounts_34', 'sanctioned_amt_for_active_accounts_34',
               'no_of_loans_opened_last_3_months_34', 'value_of_loans_opened_last_12_months_34',
               'total_number_of_accounts_34', 'no_of_loans_paid_off_successfully_34',
               'value_of_loans_opened_last_3_months_34', 'number_of_active_accounts_34', 'total_value_of_accounts_34',
               'no_of_loans_opened_last_12_months_34', 'latest_disbursed_date_34',
               'value_of_loans_paid_off_successfully_34', 'current_bal_for_active_accounts_34',
               'amt_written_off_total_liabilities_ratio_34', 'no_acs_written_off_total_liabilities_ratio_34',
               'amt_bl_paid_off_34', 'last_loan_drawn_in_months_34', 'no_acs_overdue_total_accounts_ratio_34',
               'no_of_bl_paid_off_34', 'credit_card_usage_total_limits_ratio_34', 'no_of_running_bl_pl_34',
               'no_of_enquiries_last_3_months_34', 'no_of_enquiries_last_12_months_34', 'last_account_opened_months_32',
               'monthly_emi_for_active_accounts_32', 'sanctioned_amt_for_active_accounts_32',
               'no_of_loans_opened_last_3_months_32', 'value_of_loans_opened_last_12_months_32',
               'total_number_of_accounts_32', 'no_of_loans_paid_off_successfully_32',
               'value_of_loans_opened_last_3_months_32', 'number_of_active_accounts_32', 'total_value_of_accounts_32',
               'no_of_loans_opened_last_12_months_32', 'latest_disbursed_date_32',
               'value_of_loans_paid_off_successfully_32', 'current_bal_for_active_accounts_32',
               'amt_written_off_total_liabilities_ratio_32', 'no_acs_written_off_total_liabilities_ratio_32',
               'amt_bl_paid_off_32', 'last_loan_drawn_in_months_32', 'no_acs_overdue_total_accounts_ratio_32',
               'no_of_bl_paid_off_32', 'credit_card_usage_total_limits_ratio_32', 'no_of_running_bl_pl_32',
               'no_of_enquiries_last_3_months_32', 'no_of_enquiries_last_12_months_32', 'last_account_opened_months_3',
               'monthly_emi_for_active_accounts_3', 'sanctioned_amt_for_active_accounts_3',
               'no_of_loans_opened_last_3_months_3', 'value_of_loans_opened_last_12_months_3',
               'total_number_of_accounts_3',
               'no_of_loans_paid_off_successfully_3', 'value_of_loans_opened_last_3_months_3',
               'number_of_active_accounts_3', 'total_value_of_accounts_3', 'no_of_loans_opened_last_12_months_3',
               'latest_disbursed_date_3', 'value_of_loans_paid_off_successfully_3', 'current_bal_for_active_accounts_3',
               'amt_written_off_total_liabilities_ratio_3', 'no_acs_written_off_total_liabilities_ratio_3',
               'amt_bl_paid_off_3', 'last_loan_drawn_in_months_3', 'no_acs_overdue_total_accounts_ratio_3',
               'no_of_bl_paid_off_3', 'credit_card_usage_total_limits_ratio_3', 'no_of_running_bl_pl_3',
               'no_of_enquiries_last_3_months_3', 'no_of_enquiries_last_12_months_3', 'last_account_opened_months_53',
               'monthly_emi_for_active_accounts_53', 'sanctioned_amt_for_active_accounts_53',
               'no_of_loans_opened_last_3_months_53', 'value_of_loans_opened_last_12_months_53',
               'total_number_of_accounts_53', 'no_of_loans_paid_off_successfully_53',
               'value_of_loans_opened_last_3_months_53', 'number_of_active_accounts_53', 'total_value_of_accounts_53',
               'no_of_loans_opened_last_12_months_53', 'latest_disbursed_date_53',
               'value_of_loans_paid_off_successfully_53', 'current_bal_for_active_accounts_53',
               'amt_written_off_total_liabilities_ratio_53', 'no_acs_written_off_total_liabilities_ratio_53',
               'amt_bl_paid_off_53', 'last_loan_drawn_in_months_53', 'no_acs_overdue_total_accounts_ratio_53',
               'no_of_bl_paid_off_53', 'credit_card_usage_total_limits_ratio_53', 'no_of_running_bl_pl_53',
               'no_of_enquiries_last_3_months_53', 'no_of_enquiries_last_12_months_53', 'last_account_opened_months_12',
               'monthly_emi_for_active_accounts_12', 'sanctioned_amt_for_active_accounts_12',
               'no_of_loans_opened_last_3_months_12', 'value_of_loans_opened_last_12_months_12',
               'total_number_of_accounts_12', 'no_of_loans_paid_off_successfully_12',
               'value_of_loans_opened_last_3_months_12', 'number_of_active_accounts_12', 'total_value_of_accounts_12',
               'no_of_loans_opened_last_12_months_12', 'latest_disbursed_date_12',
               'value_of_loans_paid_off_successfully_12', 'current_bal_for_active_accounts_12',
               'amt_written_off_total_liabilities_ratio_12', 'no_acs_written_off_total_liabilities_ratio_12',
               'amt_bl_paid_off_12', 'last_loan_drawn_in_months_12', 'no_acs_overdue_total_accounts_ratio_12',
               'no_of_bl_paid_off_12', 'credit_card_usage_total_limits_ratio_12', 'no_of_running_bl_pl_12',
               'no_of_enquiries_last_3_months_12', 'no_of_enquiries_last_12_months_12', 'last_account_opened_months_4',
               'monthly_emi_for_active_accounts_4', 'sanctioned_amt_for_active_accounts_4',
               'no_of_loans_opened_last_3_months_4', 'value_of_loans_opened_last_12_months_4',
               'total_number_of_accounts_4',
               'no_of_loans_paid_off_successfully_4', 'value_of_loans_opened_last_3_months_4',
               'number_of_active_accounts_4', 'total_value_of_accounts_4', 'no_of_loans_opened_last_12_months_4',
               'latest_disbursed_date_4', 'value_of_loans_paid_off_successfully_4', 'current_bal_for_active_accounts_4',
               'amt_written_off_total_liabilities_ratio_4', 'no_acs_written_off_total_liabilities_ratio_4',
               'amt_bl_paid_off_4', 'last_loan_drawn_in_months_4', 'no_acs_overdue_total_accounts_ratio_4',
               'no_of_bl_paid_off_4', 'credit_card_usage_total_limits_ratio_4', 'no_of_running_bl_pl_4',
               'no_of_enquiries_last_3_months_4', 'no_of_enquiries_last_12_months_4', 'last_account_opened_months_8',
               'monthly_emi_for_active_accounts_8', 'sanctioned_amt_for_active_accounts_8',
               'no_of_loans_opened_last_3_months_8', 'value_of_loans_opened_last_12_months_8',
               'total_number_of_accounts_8',
               'no_of_loans_paid_off_successfully_8', 'value_of_loans_opened_last_3_months_8',
               'number_of_active_accounts_8', 'total_value_of_accounts_8', 'no_of_loans_opened_last_12_months_8',
               'latest_disbursed_date_8', 'value_of_loans_paid_off_successfully_8', 'current_bal_for_active_accounts_8',
               'amt_written_off_total_liabilities_ratio_8', 'no_acs_written_off_total_liabilities_ratio_8',
               'amt_bl_paid_off_8', 'last_loan_drawn_in_months_8', 'no_acs_overdue_total_accounts_ratio_8',
               'no_of_bl_paid_off_8', 'credit_card_usage_total_limits_ratio_8', 'no_of_running_bl_pl_8',
               'no_of_enquiries_last_3_months_8', 'no_of_enquiries_last_12_months_8', 'last_account_opened_months_59',
               'monthly_emi_for_active_accounts_59', 'sanctioned_amt_for_active_accounts_59',
               'no_of_loans_opened_last_3_months_59', 'value_of_loans_opened_last_12_months_59',
               'total_number_of_accounts_59', 'no_of_loans_paid_off_successfully_59',
               'value_of_loans_opened_last_3_months_59', 'number_of_active_accounts_59', 'total_value_of_accounts_59',
               'no_of_loans_opened_last_12_months_59', 'latest_disbursed_date_59',
               'value_of_loans_paid_off_successfully_59', 'current_bal_for_active_accounts_59',
               'amt_written_off_total_liabilities_ratio_59', 'no_acs_written_off_total_liabilities_ratio_59',
               'amt_bl_paid_off_59', 'last_loan_drawn_in_months_59', 'no_acs_overdue_total_accounts_ratio_59',
               'no_of_bl_paid_off_59', 'credit_card_usage_total_limits_ratio_59', 'no_of_running_bl_pl_59',
               'no_of_enquiries_last_3_months_59', 'no_of_enquiries_last_12_months_59', 'last_account_opened_months_15',
               'monthly_emi_for_active_accounts_15', 'sanctioned_amt_for_active_accounts_15',
               'no_of_loans_opened_last_3_months_15', 'value_of_loans_opened_last_12_months_15',
               'total_number_of_accounts_15', 'no_of_loans_paid_off_successfully_15',
               'value_of_loans_opened_last_3_months_15', 'number_of_active_accounts_15', 'total_value_of_accounts_15',
               'no_of_loans_opened_last_12_months_15', 'latest_disbursed_date_15',
               'value_of_loans_paid_off_successfully_15', 'current_bal_for_active_accounts_15',
               'amt_written_off_total_liabilities_ratio_15', 'no_acs_written_off_total_liabilities_ratio_15',
               'amt_bl_paid_off_15', 'last_loan_drawn_in_months_15', 'no_acs_overdue_total_accounts_ratio_15',
               'no_of_bl_paid_off_15', 'credit_card_usage_total_limits_ratio_15', 'no_of_running_bl_pl_15',
               'no_of_enquiries_last_3_months_15', 'no_of_enquiries_last_12_months_15', 'last_account_opened_months_33',
               'monthly_emi_for_active_accounts_33', 'sanctioned_amt_for_active_accounts_33',
               'no_of_loans_opened_last_3_months_33', 'value_of_loans_opened_last_12_months_33',
               'total_number_of_accounts_33', 'no_of_loans_paid_off_successfully_33',
               'value_of_loans_opened_last_3_months_33', 'number_of_active_accounts_33', 'total_value_of_accounts_33',
               'no_of_loans_opened_last_12_months_33', 'latest_disbursed_date_33',
               'value_of_loans_paid_off_successfully_33', 'current_bal_for_active_accounts_33',
               'amt_written_off_total_liabilities_ratio_33', 'no_acs_written_off_total_liabilities_ratio_33',
               'amt_bl_paid_off_33', 'last_loan_drawn_in_months_33', 'no_acs_overdue_total_accounts_ratio_33',
               'no_of_bl_paid_off_33', 'credit_card_usage_total_limits_ratio_33', 'no_of_running_bl_pl_33',
               'no_of_enquiries_last_3_months_33', 'no_of_enquiries_last_12_months_33', 'last_account_opened_months_54',
               'monthly_emi_for_active_accounts_54', 'sanctioned_amt_for_active_accounts_54',
               'no_of_loans_opened_last_3_months_54', 'value_of_loans_opened_last_12_months_54',
               'total_number_of_accounts_54', 'no_of_loans_paid_off_successfully_54',
               'value_of_loans_opened_last_3_months_54', 'number_of_active_accounts_54', 'total_value_of_accounts_54',
               'no_of_loans_opened_last_12_months_54', 'latest_disbursed_date_54',
               'value_of_loans_paid_off_successfully_54', 'current_bal_for_active_accounts_54',
               'amt_written_off_total_liabilities_ratio_54', 'no_acs_written_off_total_liabilities_ratio_54',
               'amt_bl_paid_off_54', 'last_loan_drawn_in_months_54', 'no_acs_overdue_total_accounts_ratio_54',
               'no_of_bl_paid_off_54', 'credit_card_usage_total_limits_ratio_54', 'no_of_running_bl_pl_54',
               'no_of_enquiries_last_3_months_54', 'no_of_enquiries_last_12_months_54', 'last_account_opened_months_40',
               'monthly_emi_for_active_accounts_40', 'sanctioned_amt_for_active_accounts_40',
               'no_of_loans_opened_last_3_months_40', 'value_of_loans_opened_last_12_months_40',
               'total_number_of_accounts_40', 'no_of_loans_paid_off_successfully_40',
               'value_of_loans_opened_last_3_months_40', 'number_of_active_accounts_40', 'total_value_of_accounts_40',
               'no_of_loans_opened_last_12_months_40', 'latest_disbursed_date_40',
               'value_of_loans_paid_off_successfully_40', 'current_bal_for_active_accounts_40',
               'amt_written_off_total_liabilities_ratio_40', 'no_acs_written_off_total_liabilities_ratio_40',
               'amt_bl_paid_off_40', 'last_loan_drawn_in_months_40', 'no_acs_overdue_total_accounts_ratio_40',
               'no_of_bl_paid_off_40', 'credit_card_usage_total_limits_ratio_40', 'no_of_running_bl_pl_40',
               'no_of_enquiries_last_3_months_40', 'no_of_enquiries_last_12_months_40', 'last_account_opened_months_55',
               'monthly_emi_for_active_accounts_55', 'sanctioned_amt_for_active_accounts_55',
               'no_of_loans_opened_last_3_months_55', 'value_of_loans_opened_last_12_months_55',
               'total_number_of_accounts_55', 'no_of_loans_paid_off_successfully_55',
               'value_of_loans_opened_last_3_months_55', 'number_of_active_accounts_55', 'total_value_of_accounts_55',
               'no_of_loans_opened_last_12_months_55', 'latest_disbursed_date_55',
               'value_of_loans_paid_off_successfully_55', 'current_bal_for_active_accounts_55',
               'amt_written_off_total_liabilities_ratio_55', 'no_acs_written_off_total_liabilities_ratio_55',
               'amt_bl_paid_off_55', 'last_loan_drawn_in_months_55', 'no_acs_overdue_total_accounts_ratio_55',
               'no_of_bl_paid_off_55', 'credit_card_usage_total_limits_ratio_55', 'no_of_running_bl_pl_55',
               'no_of_enquiries_last_3_months_55', 'no_of_enquiries_last_12_months_55', 'last_account_opened_months_36',
               'monthly_emi_for_active_accounts_36', 'sanctioned_amt_for_active_accounts_36',
               'no_of_loans_opened_last_3_months_36', 'value_of_loans_opened_last_12_months_36',
               'total_number_of_accounts_36', 'no_of_loans_paid_off_successfully_36',
               'value_of_loans_opened_last_3_months_36', 'number_of_active_accounts_36', 'total_value_of_accounts_36',
               'no_of_loans_opened_last_12_months_36', 'latest_disbursed_date_36',
               'value_of_loans_paid_off_successfully_36', 'current_bal_for_active_accounts_36',
               'amt_written_off_total_liabilities_ratio_36', 'no_acs_written_off_total_liabilities_ratio_36',
               'amt_bl_paid_off_36', 'last_loan_drawn_in_months_36', 'no_acs_overdue_total_accounts_ratio_36',
               'no_of_bl_paid_off_36', 'credit_card_usage_total_limits_ratio_36', 'no_of_running_bl_pl_36',
               'no_of_enquiries_last_3_months_36', 'no_of_enquiries_last_12_months_36', 'last_account_opened_months_14',
               'monthly_emi_for_active_accounts_14', 'sanctioned_amt_for_active_accounts_14',
               'no_of_loans_opened_last_3_months_14', 'value_of_loans_opened_last_12_months_14',
               'total_number_of_accounts_14', 'no_of_loans_paid_off_successfully_14',
               'value_of_loans_opened_last_3_months_14', 'number_of_active_accounts_14', 'total_value_of_accounts_14',
               'no_of_loans_opened_last_12_months_14', 'latest_disbursed_date_14',
               'value_of_loans_paid_off_successfully_14', 'current_bal_for_active_accounts_14',
               'amt_written_off_total_liabilities_ratio_14', 'no_acs_written_off_total_liabilities_ratio_14',
               'amt_bl_paid_off_14', 'last_loan_drawn_in_months_14', 'no_acs_overdue_total_accounts_ratio_14',
               'no_of_bl_paid_off_14', 'credit_card_usage_total_limits_ratio_14', 'no_of_running_bl_pl_14',
               'no_of_enquiries_last_3_months_14', 'no_of_enquiries_last_12_months_14', 'last_account_opened_months_50',
               'monthly_emi_for_active_accounts_50', 'sanctioned_amt_for_active_accounts_50',
               'no_of_loans_opened_last_3_months_50', 'value_of_loans_opened_last_12_months_50',
               'total_number_of_accounts_50', 'no_of_loans_paid_off_successfully_50',
               'value_of_loans_opened_last_3_months_50', 'number_of_active_accounts_50', 'total_value_of_accounts_50',
               'no_of_loans_opened_last_12_months_50', 'latest_disbursed_date_50',
               'value_of_loans_paid_off_successfully_50', 'current_bal_for_active_accounts_50',
               'amt_written_off_total_liabilities_ratio_50', 'no_acs_written_off_total_liabilities_ratio_50',
               'amt_bl_paid_off_50', 'last_loan_drawn_in_months_50', 'no_acs_overdue_total_accounts_ratio_50',
               'no_of_bl_paid_off_50', 'credit_card_usage_total_limits_ratio_50', 'no_of_running_bl_pl_50',
               'no_of_enquiries_last_3_months_50', 'no_of_enquiries_last_12_months_50', 'last_account_opened_months_43',
               'monthly_emi_for_active_accounts_43', 'sanctioned_amt_for_active_accounts_43',
               'no_of_loans_opened_last_3_months_43', 'value_of_loans_opened_last_12_months_43',
               'total_number_of_accounts_43', 'no_of_loans_paid_off_successfully_43',
               'value_of_loans_opened_last_3_months_43', 'number_of_active_accounts_43', 'total_value_of_accounts_43',
               'no_of_loans_opened_last_12_months_43', 'latest_disbursed_date_43',
               'value_of_loans_paid_off_successfully_43', 'current_bal_for_active_accounts_43',
               'amt_written_off_total_liabilities_ratio_43', 'no_acs_written_off_total_liabilities_ratio_43',
               'amt_bl_paid_off_43', 'last_loan_drawn_in_months_43', 'no_acs_overdue_total_accounts_ratio_43',
               'no_of_bl_paid_off_43', 'credit_card_usage_total_limits_ratio_43', 'no_of_running_bl_pl_43',
               'no_of_enquiries_last_3_months_43', 'no_of_enquiries_last_12_months_43',
               'last_account_opened_months_None',
               'monthly_emi_for_active_accounts_None', 'sanctioned_amt_for_active_accounts_None',
               'no_of_loans_opened_last_3_months_None', 'value_of_loans_opened_last_12_months_None',
               'total_number_of_accounts_None', 'no_of_loans_paid_off_successfully_None',
               'value_of_loans_opened_last_3_months_None', 'number_of_active_accounts_None',
               'total_value_of_accounts_None',
               'no_of_loans_opened_last_12_months_None', 'latest_disbursed_date_None',
               'value_of_loans_paid_off_successfully_None', 'current_bal_for_active_accounts_None',
               'amt_written_off_total_liabilities_ratio_None', 'no_acs_written_off_total_liabilities_ratio_None',
               'amt_bl_paid_off_None', 'last_loan_drawn_in_months_None', 'no_acs_overdue_total_accounts_ratio_None',
               'no_of_bl_paid_off_None', 'credit_card_usage_total_limits_ratio_None', 'no_of_running_bl_pl_None',
               'no_of_enquiries_last_3_months_None', 'no_of_enquiries_last_12_months_None',
               'last_account_opened_months_blank', 'monthly_emi_for_active_accounts_blank',
               'sanctioned_amt_for_active_accounts_blank', 'no_of_loans_opened_last_3_months_blank',
               'value_of_loans_opened_last_12_months_blank', 'total_number_of_accounts_blank',
               'no_of_loans_paid_off_successfully_blank', 'value_of_loans_opened_last_3_months_blank',
               'number_of_active_accounts_blank', 'total_value_of_accounts_blank',
               'no_of_loans_opened_last_12_months_blank', 'latest_disbursed_date_blank',
               'value_of_loans_paid_off_successfully_blank', 'current_bal_for_active_accounts_blank',
               'amt_written_off_total_liabilities_ratio_blank', 'no_acs_written_off_total_liabilities_ratio_blank',
               'amt_bl_paid_off_blank', 'last_loan_drawn_in_months_blank', 'no_acs_overdue_total_accounts_ratio_blank',
               'no_of_bl_paid_off_blank', 'credit_card_usage_total_limits_ratio_blank', 'no_of_running_bl_pl_blank',
               'no_of_enquiries_last_3_months_blank', 'no_of_enquiries_last_12_months_blank',
               'last_account_opened_months_41', 'monthly_emi_for_active_accounts_41',
               'sanctioned_amt_for_active_accounts_41', 'no_of_loans_opened_last_3_months_41',
               'value_of_loans_opened_last_12_months_41', 'total_number_of_accounts_41',
               'no_of_loans_paid_off_successfully_41', 'value_of_loans_opened_last_3_months_41',
               'number_of_active_accounts_41', 'total_value_of_accounts_41', 'no_of_loans_opened_last_12_months_41',
               'latest_disbursed_date_41', 'value_of_loans_paid_off_successfully_41',
               'current_bal_for_active_accounts_41',
               'amt_written_off_total_liabilities_ratio_41', 'no_acs_written_off_total_liabilities_ratio_41',
               'amt_bl_paid_off_41', 'last_loan_drawn_in_months_41', 'no_acs_overdue_total_accounts_ratio_41',
               'no_of_bl_paid_off_41', 'credit_card_usage_total_limits_ratio_41', 'no_of_running_bl_pl_41',
               'no_of_enquiries_last_3_months_41', 'no_of_enquiries_last_12_months_41']

    df_cols.append('business_id')
    df_buckets = pd.DataFrame(columns=df_cols)

    i = 0

    def add_data(j):
        account_type = str(row['account_type'])

        for col in all_cols:
            # print(account_type)
            full_col = col + "_" + account_type
            # print(full_col + " : " + str(row[col]))

            df_buckets.loc[j, full_col] = row[col]

    for index, row in df_mongo_buckets.iterrows():
        business_id = row['business_id']

        all_business_id = df_buckets['business_id'].unique()
        if business_id in all_business_id:
            add_data(i)
        else:
            print(str(i))
            i += 1
            df_buckets.loc[i, 'business_id'] = business_id
            add_data(i)

    for index, row in df_buckets.iterrows():
        for col in cibil_cols_required:
            # print(col)
            if col in row:
                if str(row[col]) == "nan":
                    data_df.loc[0, col] = "0"
                else:
                    data_df.loc[0, col] = str(row[col])

            else:
                data_df.loc[0, col] = "0"

            # data_df = data_df.replace("nan", "0")

    # get cursor from cas_business
    db = db_init()
    cas_business = db['cas_business']
    data = cas_business.find({"business_id": business_id}).sort("_id", pymongo.DESCENDING).limit(1)

    for i, row in enumerate(data):
        date_of_birth = str(row['business_partners'][0]['date_of_birth'])
        print("date_of_birth : " + str(date_of_birth))
        latest_cibil_score = row['latest_cibil_score']

    date_of_birth = parse(date_of_birth)
    now = datetime.datetime.now()

    age = now - date_of_birth
    print(age.days)
    from datetime import date, timedelta

    age = age.days / 365.2425

    if int(age) == 21 or int(age) == 35 or int(age) == 40:
        age = str(int(age) + 1)

    print(int(age))

    # ade age to the data_df
    data_df.loc[0, 'age'] = str(age)

    # #     get model
    # #     ############
    file_name = "op"
    tmp_file = tempfile.NamedTemporaryFile(prefix=file_name, suffix='.csv')
    data_df.to_csv(tmp_file.name, index=False)
    temporary_file_util.put_temp_file(file_name, tmp_file)

    # ##########
    return get_scorecard(file_name)


def get_scorecard(file_name):
    tmp_file = temporary_file_util.get_temp_file(file_name)

    final_iv = pd.read_csv(
        'service_api_scorecard/final_df_df_output.csv')

    X_test = pd.read_csv(tmp_file.name)

    # X_test= X_test.fillna(0)

    # print(X_test.columns)
    # print(final_iv.columns)

    import pickle

    filename = 'service_api_scorecard/LogisticRegression.sav'
    logreg_clf = pickle.load(open(filename, 'rb'))

    X_test_woe = woe_conversion(X_test, final_iv)
    X_test_score_tab, X_test_scored = scorecard(X_test_woe, logreg_clf, final_iv, 600, 50, 20)

    # pred_logreg_test = logreg_clf.predict(X_test)
    # print(X_test_scored)
    # print(X_test_score_tab)
    # print(X_test_scored[['total_score']])
    # X_test_woe.to_csv('service_api_scorecard/X_test_woe.csv', index=False)
    # X_test_scored.to_csv('service_api_scorecard/X_test_scored.csv', index=False)
    # X_test_score_tab.to_csv('service_api_scorecard/X_test_score_tab.csv', index=False)

    print(type(X_test_scored[['total_score']]))
    response = str(X_test_scored.iloc[0]['total_score'])
    print(response)
    return response
