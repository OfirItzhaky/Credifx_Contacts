# ##########################################Description#####################################################
# # script will get a list of market ids as an input, and will return a file with potential contacts
# #Parameters:
# 1. ProprtyType - MultiFamily/Mixed/Retail
# 2. Loan size or DEED size 1mm-20mm
# 3 Contact - Phone or Email , Priority 0, 600, 1000
#     first contact by ranking and second only if description include : acquisition, invest, cfo, finance
# #   no more than 1 unique contacts in the file
# ad  census tract type
# ###########################################Library Imports################################################
import datetime
import json
import math
import time
import urllib
from functools import partial
import numpy as np
import pandas as pd
from bson import ObjectId
from pymongo import MongoClient
import xlsxwriter
pd.set_option('display.max_columns', 500)


################################## Aid functions ##################################################
def get_value(key, entity):
    try:
        res = entity.get(key)
    except:
        res = None
    return res


def get_value_Arr(key, th, entity):
    try:
        res = entity[th].get(key)
    except:
        res = None
    return res


################################## Client connect to DB ##################################################

def connect_to_MongoDB():
    password = urllib.quote_plus('FrsxcphH76')
    client = MongoClient('mongodb://readonly:' + password + '@prodclone.corp.credfi.com')
    return client

########################### pull  from user input #####################################################
def pull_input_to_list(filename):
    df_market_input = pd.read_csv(filename, low_memory=False)
    market_list = list(df_market_input['MarketId'].apply(ObjectId))  # create a list of company ids from input
    market_list = set(market_list)  # clean list of duplicates
    market_list = [i for i in market_list if i is not np.nan]  # clean list out of NAN values
    return market_list

def pull_properties_init(client, market_list):

    db = client['UISupport']['PropertyDetail']
    pipe =  [
    {'$match': {"Markets._id": {'$in': market_list}}},
    {'$project': {'_id': 1,"PurchasePrice": 1}}
    ]
    cursor_property_ids = db.aggregate(pipe, allowDiskUse=True)
    property_ids = pd.DataFrame(list(cursor_property_ids))
    return property_ids

def pull_properties_loans(client, df_property_ids):
    db = client['UISupport']['LoanDetails']
    pipe = [
        {'$match': {"Properties._id": {'$in': df_property_ids["_id"].tolist()}}},
        {'$match': {"Terms.InitialBalance": {"$gte": 1000000, "$lte": 20000000}}},
        {'$unwind': '$Properties'},
        {'$match': {"Properties._id": {'$in': df_property_ids["_id"].tolist()}}},
        {'$project': {"_id": 1, "Properties._id": 1, "Terms.InitialBalance": 1}}
    ]
    cursor_property_loans = db.aggregate(pipe, allowDiskUse=True)
    property_loans = pd.DataFrame(list(cursor_property_loans))
    return property_loans

def find_relevant_properties(df_property_ids,df_properties_loans):
    df_properties_loans['InitialBalance'] = df_properties_loans['Terms'].apply(partial(get_value, 'InitialBalance'))
    #df_properties_loans['InitialBalance'] = df_properties_loans['Terms'].apply(get_value('InitialBalance',df_properties_loans['Terms'])) --check why it's not working...
    df_properties_loans['Properties_id'] = df_properties_loans['Properties'].apply(partial(get_value, '_id')).astype(str)
    df_property_ids["_id"] = df_property_ids["_id"].astype(str)
    # df_properties_loans.sort_values("Properties_id",inplace=True)
    # df_property_ids.sort_values("_id",inplace=True)
    df_property_ids_loans = pd.merge(df_property_ids, df_properties_loans, how='left', left_on='_id', right_on='Properties_id')
    df_property_ids_loans.drop(labels=["Properties", "Terms","Properties_id"], axis="columns", inplace=True)
    df_property_ids_loans.rename(columns={'_id_y': 'Loan_id','_id_x':"Properties_id"},inplace=True)
    mask1 = df_property_ids_loans["PurchasePrice"].between(1000000, 20000000)
    mask2 = df_property_ids_loans["InitialBalance"].between(1000000, 20000000)
    df_property_ids_loans = df_property_ids_loans[mask1 | mask2]
    df_property_ids_loans["Origin"] = ""
    df_property_ids_loans["Origin"][mask1 & mask2] = "Both"
    df_property_ids_loans["Origin"][~mask2 & mask1] = "Deed"
    df_property_ids_loans["Origin"][~mask1 & mask2] = "Loan"
    return df_property_ids_loans

def extract_loan_info(client,loan_ids_list):
    db = client['UISupport']['LoanDetails']

    pipe = [
        {'$match': {"_id": {'$in': loan_ids_list}}},
        {'$project': {"_id": 1, "Originators": 1, "Terms.InitialBalance": 1,
                      "Terms.OriginationDate": 1,
                      "Terms.InterestRate": 1, "Terms.MaturityDate": 1, "IsCurrent": 1, "Terms.ReleaseDate": 1,
                      "IsCrossCollateralized": 1,
                      "Borrowers": 1, "Lenders": 1, "CMBSList": 1}}
    ]
    cursor_loan_details = db.aggregate(pipe, allowDiskUse=True)
    df_loan_info = pd.DataFrame(list(cursor_loan_details))

    df_loan_info['ORIGINATORS_ID'] = df_loan_info['Originators'].apply(partial(get_value_Arr, '_id',0))
    df_loan_info['ORIGINATORS_NAME'] = df_loan_info['Originators'].apply(partial(get_value_Arr, 'Name',0))
    df_loan_info['INITIAL_BALANCE'] = df_loan_info['Terms'].apply(partial(get_value, 'InitialBalance'))
    df_loan_info['ORIGINATION_DATE'] = df_loan_info['Terms'].apply(partial(get_value, 'OriginationDate'))
    df_loan_info['INTEREST_RATE'] = df_loan_info['Terms'].apply(partial(get_value, 'InterestRate')) / 100.0
    df_loan_info['MATURITY_DATE'] = df_loan_info['Terms'].apply(partial(get_value, 'MaturityDate'))
    df_loan_info['RELEASE_DATE'] = df_loan_info['Terms'].apply(partial(get_value, 'ReleaseDate'))
    df_loan_info['BORROWERS_ID'] = df_loan_info['Borrowers'].apply(partial(get_value_Arr, '_id', 0)).astype(str)
    df_loan_info['BORROWERS_NAME'] = df_loan_info['Borrowers'].apply(partial(get_value_Arr, 'Name', 0)).str.upper()
    df_loan_info['CURRENT_LENDER_ID'] = df_loan_info['Lenders'].apply(partial(get_value_Arr, '_id', 0)).astype(str)
    df_loan_info['CURRENT_LENDER_NAME'] = df_loan_info['Lenders'].apply(partial(get_value_Arr, 'Name', 0)).str.upper()
    df_loan_info['CMBSLIST_PRIMARY_DEAL_TYPE'] = df_loan_info['CMBSList'].apply(
        partial(get_value_Arr, 'PrimaryDealType', 0)).str.upper()
    df_loan_info['CMBSLIST_CMBS_NAME'] = df_loan_info['CMBSList'].apply(partial(get_value_Arr, 'Cmbs', 0)).apply(
        partial(get_value, 'Name')).str.upper()
    df_loan_info.drop(["Originators", "Terms", "Borrowers", "Lenders", "CMBSList"], axis=1, inplace=True)

    return df_loan_info

# MERGING 2 DATAFRAMES DIFFERENT KEYS
def merge_two_dataframes_left(df_a, df_b, left_on_a, right_on_b):
    if left_on_a == right_on_b:
        merged_df = pd.merge(df_a, df_b, how="left", on=left_on_a)
    else:
        merged_df = pd.merge(df_a, df_b, how="left", left_on=left_on_a, right_on=right_on_b)
        merged_df.drop(right_on_b, axis=1, inplace=True)  # drop the column that was added we dont need

    return merged_df

def merge_two_dataframes_inner(df_a, df_b, left_on_a, right_on_b):
    if left_on_a == right_on_b:
        merged_df = pd.merge(df_a, df_b, how="inner", on=left_on_a)
    else:
        merged_df = pd.merge(df_a, df_b, how="inner", left_on=left_on_a, right_on=right_on_b)
        merged_df.drop(right_on_b, axis=1, inplace=True)  # drop the column that was added we dont need

    return merged_df

def extract_property_info(client,properties_ids_list):
    db = client['UISupport']['PropertyDetail']
    pipe = [
        {'$match': {"_id": {'$in': properties_ids_list}}},
        {'$project': {"_id": 1, "PropertyName": 1, "Address": 1,
                      "PropertyType": 1, "PropertySubType": 1,
                      "IsAffordableHousing": 1, "AreaTotalsTotal": 1, "ResolvedUnitsTotal": 1, "YearBuilt": 1,
                      "Renovation1": 1, "Renovation2": 1,
                      "Owners": 1, "PurchaseDate": 1, "PurchasePrice": 1}}
    ]
    cursor_property_details = db.aggregate(pipe, allowDiskUse=True)
    df_property_info = pd.DataFrame(list(cursor_property_details))


    df_property_info['PROPERTY_STREET'] = df_property_info['Address'].apply(partial(get_value, 'Street')).str.upper()
    df_property_info['PROPERTY_CITY'] = df_property_info['Address'].apply(partial(get_value, 'City')).str.upper()
    df_property_info['PROPERTY_STATE'] = df_property_info['Address'].apply(partial(get_value, 'State')).str.upper()
    df_property_info['PROPERTY_ZIPCODE'] = df_property_info['Address'].apply(partial(get_value, 'ZipCode')).str.upper()
    df_property_info['BUYER_1_ID'] = df_property_info['Owners'].apply(partial(get_value_Arr, '_id', 0))
    df_property_info['BUYER_1_NAME'] = df_property_info['Owners'].apply(partial(get_value_Arr, 'Name', 0)).str.upper()
    df_property_info['BUYER_2_ID'] = df_property_info['Owners'].apply(partial(get_value_Arr, '_id', 1))
    df_property_info['BUYER_2_NAME'] = df_property_info['Owners'].apply(partial(get_value_Arr, 'Name', 1)).str.upper()
    df_property_info.drop(["Address", "Owners"], axis=1, inplace=True)

    df_property_info["Renovation2"][df_property_info["Renovation1"] == df_property_info["Renovation2"]] = ""    #Removes values from renovation 2 column when its equal to renovation 1
    pipe_msa = [
        {'$match': {"_id": {'$in': properties_ids_list}}},
        {'$unwind': '$Markets'},
        {'$match': {"Markets.Type": "MsaMarket"}},
        {'$project': {"_id": 1, "Markets": 1}}]

    cursor_property_msa = db.aggregate(pipe_msa, allowDiskUse=True)
    df_property_info_msa = pd.DataFrame(list(cursor_property_msa))

    df_property_info_msa['MSA'] = df_property_info_msa['Markets'].apply(partial(get_value, 'Name')).str.upper()
    df_property_info_msa.drop("Markets", axis=1, inplace=True)
    df_property_info = merge_two_dataframes_left(df_property_info, df_property_info_msa, "_id", "_id")

    pipe_census_tract = [
        {'$match': {"_id": {'$in': properties_ids_list}}},
        {'$unwind': '$Markets'},
        {'$match': {"Markets.Type": "CensusTractMarket"}},
        {'$project': {"_id": 1, "Markets": 1}}]

    cursor_property_census_tract = db.aggregate(pipe_census_tract, allowDiskUse=True)
    df_property_info_census_tract = pd.DataFrame(list(cursor_property_census_tract))

    df_property_info_census_tract['CENSUS_TRACT_NUMBER'] = df_property_info_census_tract['Markets'].apply(partial(get_value, 'Name')).str.upper()
    df_property_info_census_tract.drop("Markets", axis=1, inplace=True)
    df_property_info = merge_two_dataframes_left(df_property_info, df_property_info_census_tract, "_id", "_id")

    df_census_tract_description = pd.read_csv("S:\Census Tract\Census Tract Description Fixed.csv", low_memory=False)
    df_property_info["CENSUS_TRACT_NUMBER"] = df_property_info["CENSUS_TRACT_NUMBER"].astype(float)
    df_property_info = merge_two_dataframes_left(df_property_info, df_census_tract_description, "CENSUS_TRACT_NUMBER", "census_tract")

    df_property_info.drop_duplicates(subset=["_id"], keep="first", inplace = True)


    return df_property_info

def pull_contacts(client,df_properties_and_loans_full_info):

    db = client['UISupport']['PropertyCompanyRelations']
    prop_list = df_properties_and_loans_full_info["Properties_id"].drop_duplicates(keep="first").dropna().apply(ObjectId).tolist()

    pcr_pipe = [
        {'$match': {'Property._id': {'$in': prop_list},
                    '$or': [{'Priority': {'$lt': 50}}, {'Priority': {'$gte': 600}}]}},
        {'$project': {"Property": 1, "Company": 1, "Priority": 1}}
    ]

    db = client['UISupport']['PropertyCompanyRelations']
    df_pcr = pd.DataFrame(list(db.aggregate(pcr_pipe, allowDiskUse=True)))
    df_pcr['PROPERTIES_ID'] = df_pcr['Property'].apply(partial(get_value, '_id'))
    df_pcr['COMPANY_ID'] = df_pcr['Company'].apply(partial(get_value, '_id'))
    df_pcr.drop(["Company", "_id", "Property"], axis=1, inplace=True)
    company_list =df_pcr['COMPANY_ID'].drop_duplicates(keep="first").dropna().apply(ObjectId).tolist()


    cer_pipe = [
        {'$match': {'Company._id': {'$in': company_list}}},
        {'$project': {"Company": 1, "Employee": 1, "EmployeeRanking": 1,"Phone": 1, "Email": 1,"Role": 1, "RoleDescription": 1}}
    ]

    db = client['UISupport']['CompanyEmployeeRelations']
    df_cer = pd.DataFrame(list(db.aggregate(cer_pipe, allowDiskUse=True)))
    df_cer['COMPANY_ID'] = df_cer['Company'].apply(partial(get_value, '_id'))
    df_cer['COMPANY_NAME'] = df_cer['Company'].apply(partial(get_value, 'Name'))
    df_cer['EMPLOYEE_ID'] = df_cer['Employee'].apply(partial(get_value, '_id'))
    df_cer['EMPLOYEE_NAME'] = df_cer['Employee'].apply(partial(get_value, 'Name'))
    df_cer.drop(["Company", "_id", "Employee"], axis=1, inplace=True)
    df_pcr['COMPANY_ID'] = df_pcr['COMPANY_ID'].astype(str)
    df_cer['COMPANY_ID'] = df_cer['COMPANY_ID'].astype(str)

    df_all_contacts = merge_two_dataframes_left(df_pcr, df_cer, 'COMPANY_ID', "COMPANY_ID")
    mask1 = df_all_contacts["Phone"].notnull()
    mask2 = df_all_contacts["Email"].notnull()
    df_all_contacts = df_all_contacts[mask2 | mask1]
    df_all_contacts.sort_values(["Priority", "EmployeeRanking"] , ascending=[False, True], inplace=True)

    mask3 = df_all_contacts["EmployeeRanking"] == 1
    df_first_contact = df_all_contacts[mask3].copy()

    df_first_contact.drop_duplicates(subset=["PROPERTIES_ID"], keep="first", inplace = True)
    df_first_contact.drop(["EmployeeRanking"], axis=1, inplace=True)    #"Priority",
    df_first_contact.rename(
        columns={'PROPERTIES_ID': 'FIRST_CONTACT_PROPERTIES_ID', 'COMPANY_ID': 'FIRST_CONTACT_COMPANY_ID',
                 "EMPLOYEE_ID": 'FIRST CONTACT EMPLOYEE ID', "EMPLOYEE_NAME": 'FIRST CONTACT EMPLOYEE NAME',
                 'Email': 'FIRST_CONTACT_EMAIL','Phone': 'FIRST_CONTACT_PHONE',
                 'PROPERTIES_ID': 'FIRST_CONTACT_PROPERTIES_ID','Role': 'FIRST_CONTACT_ROLE',
                 'RoleDescription': 'FIRST_CONTACT_ROLE_DESCRIPTION', 'COMPANY_NAME': 'FIRST_CONTACT_COMPANY_NAME'}, inplace=True)

    df_properties_and_loans_full_info['Properties_id'] = df_properties_and_loans_full_info['Properties_id'].astype(str)
    df_first_contact['FIRST_CONTACT_PROPERTIES_ID'] =  df_first_contact['FIRST_CONTACT_PROPERTIES_ID'].astype(str)
    df_full_with_first_contact = merge_two_dataframes_inner(df_properties_and_loans_full_info,df_first_contact,"Properties_id","FIRST_CONTACT_PROPERTIES_ID")

    mask4 = df_all_contacts["EmployeeRanking"] != 1
    mask5 = df_all_contacts['RoleDescription'].str.lower().str.contains("invest|finance|acquisition|cfo") == True

    df_second_contact = df_all_contacts[mask4&mask5].copy()

    df_second_contact.drop(["Priority", "EmployeeRanking"], axis=1, inplace=True)
    df_second_contact.rename(
        columns={'PROPERTIES_ID': 'SECOND_CONTACT_PROPERTIES_ID', 'COMPANY_ID': 'SECOND_CONTACT_COMPANY_ID',
                 "EMPLOYEE_ID": 'SECOND CONTACT EMPLOYEE ID', "EMPLOYEE_NAME": 'SECOND CONTACT EMPLOYEE NAME',
                 'Email': 'SECOND_CONTACT_EMAIL', 'Phone': 'SECOND_CONTACT_PHONE',
                  'Role': 'SECOND_CONTACT_ROLE', 'RoleDescription': 'SECOND_CONTACT_ROLE_DESCRIPTION', 'COMPANY_NAME': 'SECOND_CONTACT_COMPANY_NAME'},
        inplace=True)

    df_second_contact['SECOND_CONTACT_PROPERTIES_ID'] = df_second_contact['SECOND_CONTACT_PROPERTIES_ID'].astype(str)
    df_full_with_first_contact["Properties_id"] = df_full_with_first_contact["Properties_id"].astype(str)

    df_second_contact["concat_property_company"] = df_second_contact["SECOND_CONTACT_PROPERTIES_ID"].astype(str) + df_second_contact["SECOND_CONTACT_COMPANY_ID"].astype(str)
    df_second_contact.drop_duplicates(subset=["concat_property_company"], keep="first", inplace=True)

    df_full_with_first_contact["concat_property_company"] = df_full_with_first_contact["Properties_id"].astype(str) + df_full_with_first_contact["FIRST_CONTACT_COMPANY_ID"].astype(str)

    df_full_with_both_contacts = merge_two_dataframes_left(df_full_with_first_contact, df_second_contact,
                                                            "concat_property_company", "concat_property_company")

    df_full_with_both_contacts.drop(["concat_property_company", "SECOND_CONTACT_PROPERTIES_ID"], axis=1, inplace=True)
    df_full_with_both_contacts.rename(
        columns={'FIRST_CONTACT_COMPANY_ID': 'CONTACTS_COMPANY_ID'}, inplace=True)
    return df_full_with_both_contacts

def init_output_df(companies_list):
    df_output = pd.DataFrame()
    df_output['child_id'] = companies_list
    df_output.dropna(subset=["child_id"], inplace=True)
    df_output.drop_duplicates(keep="first", inplace=True)
    df_output['max_level_of_parent'] = 0
    df_output['parent_id'] = 0
    df_output.set_index('child_id', inplace=True)
    return df_output


def pull_ultimate_parent(client, df_output,companies_list):
    companies = client['UISupport']['Ownership']
    corporate_structure = companies.find({'$and': [{"Source": {"$in": companies_list}}, {"Level": {"$gt": 0}}]},
                                         {'Source': 1, 'ParentId': 1, 'ChildId': 1, 'Level': 1})  # .limit(25)
    df_corporate_structure = pd.DataFrame(list(
        corporate_structure))  # pulls all the companies with their corporate structure only levels greater than zero
    for index, row in df_corporate_structure.iterrows():
        # print (row['Level'])
        # print (df_output.ix[row['Source'], 'max_level_of_parent'])
        if row['Level'] > df_output.ix[row['Source'], 'max_level_of_parent']:
            df_output.ix[row['Source'], 'max_level_of_parent'] = row['Level']
            df_output.ix[row['Source'], 'parent_id'] = row['ParentId']
        else:
            pass
    df_output.reset_index(inplace=True)
    for i in range(0, len(df_output)):  # checking if no parent was found place child id also in the parents columns
        if df_output.ix[i, 'max_level_of_parent'] == 0:
            df_output.ix[i, 'parent_id'] = df_output.ix[i, 'child_id']


# HOLDING COMPANIES -PULL CHILDREN COMPANIES NAMES:
def pull_child_company_name(client, df_output):
    childrens_names = client['CorporateStructure']['Companies']
    childrens = childrens_names.find({"_id": {"$in": df_output['child_id'].tolist()}},
                                     {'_id': 1, 'Name': 1})  # .limit(25)
    df_childrens_names = pd.DataFrame(list(childrens))
    df_childrens_names.rename(columns={'Name': 'child_company_name'}, inplace=True)
    return df_childrens_names


# HOLDING COMPANIES -PULL PARENT COMPANIES NAMES:
def pull_parent_company_name(client, df_output):
    parents_names = client['CorporateStructure']['Companies']
    parents = parents_names.find({"_id": {"$in": df_output['parent_id'].tolist()}},
                                 {'_id': 1, 'Name': 1})  # .limit(25)
    df_parents_names = pd.DataFrame(list(parents))
    df_parents_names.rename(columns={'Name': 'holding_company_name'}, inplace=True)
    return df_parents_names


# HOLDING COMPANIES - MERGING 2 DATAFRAMES
def merge_two_dataframes_holding_company(df_a, df_b, left_on_a, right_on_b):
    merged_df = pd.merge(df_a, df_b, left_on=left_on_a, right_on=right_on_b)
    merged_df.drop(right_on_b, axis=1, inplace=True)  # drop the column that was added we dont need '_id'
    return merged_df





def pull_parents_info(client, df_full):

    company_list_holding_company = df_full["ORIGINATORS_ID"].dropna().tolist() + df_full["BUYER_1_ID"].dropna().tolist() + df_full["BUYER_2_ID"].dropna().tolist() + df_full["BORROWERS_ID"].dropna().tolist()
    df_output = init_output_df(company_list_holding_company)
    print("Holding_Company: Pulling Ultimate Parent")
    pull_ultimate_parent(client, df_output, company_list_holding_company)
    print("Holding_Company: Pulling child company name")
    df_childrens_names = pull_child_company_name(client, df_output)
    print("Holding_Company: Pulling parent company name")
    df_parents_names = pull_parent_company_name(client, df_output)
    print("Holding_Company: merge child company company name")
    df_output = merge_two_dataframes_holding_company(df_output, df_childrens_names, "child_id", "_id")
    print("Holding_Company: merge parent company company name")
    df_output = merge_two_dataframes_holding_company(df_output, df_parents_names, "parent_id", "_id")

    df_output_parents_only = df_output[df_output['max_level_of_parent'] != 0]
    df_output_parents_only["child_id"] = df_output_parents_only["child_id"].astype(str)

    df_full["ORIGINATORS_ID"] = df_full["ORIGINATORS_ID"].astype(str)
    df_full = merge_two_dataframes_left(df_full, df_output_parents_only, "ORIGINATORS_ID", "child_id")
    df_full.drop(["max_level_of_parent", "child_company_name"], axis=1, inplace=True)
    df_full.rename(columns={'parent_id': 'HOLDING_COMPANY_ID', 'holding_company_name': 'HOLDING_COMPANY_NAME'}, inplace=True)

    df_full["BORROWERS_ID"] = df_full["BORROWERS_ID"].astype(str)
    df_full = merge_two_dataframes_left(df_full, df_output_parents_only, "BORROWERS_ID", "child_id")
    df_full.drop(["max_level_of_parent", "child_company_name"], axis=1, inplace=True)
    df_full.rename(columns={'parent_id': 'BORROWER_PARENT_ID', 'holding_company_name': 'BORROWER_PARENT_NAME'}, inplace=True)

    df_full["BUYER_1_ID"] = df_full["BUYER_1_ID"].astype(str)
    df_full = merge_two_dataframes_left(df_full, df_output_parents_only, "BUYER_1_ID", "child_id")
    df_full.drop(["max_level_of_parent", "child_company_name"], axis=1, inplace=True)
    df_full.rename(columns={'parent_id': 'ULTIMATE_PARENT_1_ID', 'holding_company_name': 'ULTIMATE_PARENT_1_NAME'}, inplace=True)

    df_full["BUYER_2_ID"] = df_full["BUYER_2_ID"].astype(str)
    df_full = merge_two_dataframes_left(df_full, df_output_parents_only, "BUYER_2_ID", "child_id")
    df_full.drop(["max_level_of_parent", "child_company_name"], axis=1, inplace=True)
    df_full.rename(columns={'parent_id': 'ULTIMATE_PARENT_2_ID', 'holding_company_name': 'ULTIMATE_PARENT_2_NAME'},
                   inplace=True)
    return df_full

def format_and_export_to_excel(df_final,time):

    df_final.replace("None", "", inplace=True)
    df_final.replace("nan", "", inplace=True)
    df_final.fillna("", inplace=True)

    df_final["INTEREST_RATE"][df_final["INTEREST_RATE"] >= 0.1] = ""

    df_final["IsCurrent"] = df_final["IsCurrent"].astype(str)
    df_final["IsCrossCollateralized"] = df_final["IsCrossCollateralized"].astype(str)
    df_final["IsAffordableHousing"] = df_final["IsAffordableHousing"].astype(str)

    df_final["IsCurrent"].replace(["True","False"], ["YES", "NO"], inplace=True)
    df_final["IsCrossCollateralized"].replace(["True","False"], ["YES", "NO"], inplace=True)
    df_final["IsAffordableHousing"].replace(["True","False"], ["YES", "NO"], inplace=True)

    df_final.rename(columns={'PurchasePrice_x': 'PURCHASE PRICE', 'Properties_id': 'PROPERTY ID','Loan_id': 'LOAN ID', 'Origin': 'ORIGIN - DEED/LOAN',
                             'IsCrossCollateralized': 'CROSS COLLATERALIZED', 'IsCurrent': 'IS CURRENT', 'ORIGINATORS_ID': 'ORIGINATORS ID', 'ORIGINATORS_NAME': 'ORIGINATORS NAME', 'INITIAL_BALANCE': 'INITIAL BALANCE'
                             , 'ORIGINATION_DATE': 'ORIGINATION DATE', 'INTEREST_RATE': 'INTEREST RATE', 'MATURITY_DATE': 'MATURITY DATE', 'RELEASE_DATE': 'RELEASE DATE', 'BORROWERS_ID': 'BORROWERS ID',
                              'BORROWERS_NAME': 'BORROWERS NAME', 'CURRENT_LENDER_ID': 'CURRENT LENDER ID', 'CURRENT_LENDER_NAM': 'CURRENT LENDER NAME', 'CMBSLIST_PRIMARY_DEAL_TYPE': 'CMBSLIST PRIMARY DEAL TYPE', 'CMBSLIST_CMBS_NAME': 'CMBSLIST CMBS NAME',
                             'AreaTotalsTotal': 'AREA TOTAL', 'IsAffordableHousing': 'ISAFFORDABLEHOUSING', 'PropertyName': 'PROPERTY NAME', 'PropertySubType': 'PROPERTY SUBTYPE', 'PropertyType': 'PROPERTY TYPE', 'PurchaseDate': 'PURCHASE DATE',
                             'Renovation1': 'RENOVATION YEAR 1', 'Renovation2': 'RENOVATION YEAR 2', 'ResolvedUnitsTotal': 'NUMBER OF UNITS TOTAL', 'YearBuilt': 'YEAR BUILT', 'PROPERTY_STREET': 'PROPERTY STREET',
                             'PROPERTY_CITY': 'PROPERTY CITY', 'PROPERTY_STATE': 'PROPERTY STATE', 'PROPERTY_ZIPCODE': 'PROPERTY ZIPCODE', 'BUYER_1_ID': 'BUYER 1 ID', 'BUYER_1_NAME': 'BUYER 1 NAME', 'BUYER_2_ID': 'BUYER 2 ID',
                             'BUYER_2_NAME': 'BUYER 2 NAME','CENSUS_TRACT_NUMBER': 'CENSUS TRACT NUMBER', 'Description': 'TRACT TYPE',"CONTACTS_COMPANY_ID": "CONTACTS COMPANY ID", 'FIRST_CONTACT_EMAIL': 'FIRST CONTACT EMAIL', 'FIRST_CONTACT_PHONE': 'FIRST CONTACT PHONE',
                             'FIRST_CONTACT_ROLE': 'FIRST CONTACT ROLE', 'FIRST_CONTACT_ROLE_DESCRIPTION': 'FIRST CONTACT ROLE DESCRIPTION', 'FIRST_CONTACT_COMPANY_NAME': 'CONTACTS COMPANY NAME', 'SECOND_CONTACT_EMAIL': 'SECOND CONTACT EMAIL', 'SECOND_CONTACT_PHONE': 'SECOND CONTACT PHONE',
                             'SECOND_CONTACT_ROLE': 'SECOND CONTACT ROLE', 'SECOND_CONTACT_ROLE_DESCRIPTION': 'SECOND CONTACT ROLE DESCRIPTION', 'HOLDING_COMPANY_ID': 'HOLDING COMPANY ID', 'HOLDING_COMPANY_NAME': 'HOLDING COMPANY NAME', 'BORROWER_PARENT_ID': 'ULTIMATE BORROWER ID',
                             'BORROWER_PARENT_NAME': 'ULTIMATE BORROWER NAME', 'ULTIMATE_PARENT_1_ID': 'ULTIMATE OWNER 1 ID', 'ULTIMATE_PARENT_1_NAME': 'ULTIMATE OWNER 1 NAME', 'ULTIMATE_PARENT_1_ID': 'ULTIMATE OWNER 1 ID', 'ULTIMATE_PARENT_1_NAME': 'ULTIMATE OWNER 1 NAME',
                             'CURRENT_LENDER_NAME': 'CURRENT LENDER NAME',"ULTIMATE_PARENT_2_ID": "ULTIMATE OWNER 2 ID", "ULTIMATE_PARENT_2_NAME": "ULTIMATE OWNER 2 NAME"}, inplace=True)

    df_final.drop(labels=["InitialBalance", "PurchasePrice_y", "SECOND_CONTACT_COMPANY_ID", "SECOND_CONTACT_COMPANY_NAME", "Priority"], axis="columns", inplace=True)



    df_final = df_final[['CONTACTS COMPANY ID', 'CONTACTS COMPANY NAME', 'FIRST CONTACT EMPLOYEE ID', 'FIRST CONTACT EMPLOYEE NAME', 'FIRST CONTACT ROLE', 'FIRST CONTACT ROLE DESCRIPTION',
                         'FIRST CONTACT PHONE', 'FIRST CONTACT EMAIL', 'SECOND CONTACT EMPLOYEE ID', 'SECOND CONTACT EMPLOYEE NAME', 'SECOND CONTACT ROLE', 'SECOND CONTACT ROLE DESCRIPTION',
                         'SECOND CONTACT PHONE', 'SECOND CONTACT EMAIL', 'ORIGIN - DEED/LOAN', 'PROPERTY ID', 'PROPERTY NAME', 'PROPERTY STREET', 'PROPERTY CITY', 'MSA', 'PROPERTY STATE',
                         'PROPERTY ZIPCODE', 'PROPERTY TYPE', 'PROPERTY SUBTYPE', 'ISAFFORDABLEHOUSING', 'AREA TOTAL', 'NUMBER OF UNITS TOTAL', 'YEAR BUILT', 'RENOVATION YEAR 1',
                         'RENOVATION YEAR 2', 'CENSUS TRACT NUMBER', 'TRACT TYPE', 'BUYER 1 ID', 'BUYER 1 NAME', 'ULTIMATE OWNER 1 ID', 'ULTIMATE OWNER 1 NAME', 'BUYER 2 ID', 'BUYER 2 NAME',
                         'ULTIMATE OWNER 2 ID', 'ULTIMATE OWNER 2 NAME', 'PURCHASE DATE', 'PURCHASE PRICE', 'LOAN ID', 'HOLDING COMPANY ID', 'HOLDING COMPANY NAME', 'ORIGINATORS ID', 'ORIGINATORS NAME',
                         'INITIAL BALANCE', 'ORIGINATION DATE', 'INTEREST RATE', 'MATURITY DATE', 'IS CURRENT', 'RELEASE DATE', 'CROSS COLLATERALIZED', 'BORROWERS ID', 'BORROWERS NAME', 'ULTIMATE BORROWER ID',
                         'ULTIMATE BORROWER NAME', 'CURRENT LENDER ID', 'CURRENT LENDER NAME', 'CMBSLIST PRIMARY DEAL TYPE', 'CMBSLIST CMBS NAME']]

    list_uppercase_columns = ["CONTACTS COMPANY NAME", "PROPERTY NAME", "PROPERTY STREET", "PROPERTY CITY", "MSA",
                              "BUYER 1 NAME", "ULTIMATE OWNER 1 NAME", "ULTIMATE OWNER 2 NAME"] ###partial list needs to update...

    for i in list_uppercase_columns:            #change some columns to uppercase
        df_final[i] = df_final[i].str.upper()



    df_final["FIRST CONTACT EMAIL"] =  df_final["FIRST CONTACT EMAIL"].str.lower()
    df_final["SECOND CONTACT EMAIL"] = df_final["SECOND CONTACT EMAIL"].str.lower()

    writer1 = pd.ExcelWriter('Credifi_potential_Contacts_'+ time + '.xlsx', engine='xlsxwriter',
                             datetime_format='mmmm d, yyyy')
    sheetname1 = "Data"
    workbook = writer1.book
    df_final.to_excel(writer1, index=False, sheet_name=sheetname1)
    worksheet = writer1.sheets[sheetname1]




    ##### Add some cell formats
    url_format = workbook.add_format({'font_color': 'blue', 'underline': 1})
    percent_format = workbook.add_format({'num_format': '0.00%'})
    money = workbook.add_format({'num_format': '$#,##0'})
    number_format = workbook.add_format({'num_format': '#,###'})

    worksheet.set_column('Z:Z', 10, number_format)
    worksheet.set_column('AA:AA', 10, number_format)
    worksheet.set_column('AE:AE', 20)
    worksheet.set_column('AP:AP', 10, money)
    worksheet.set_column('AV:AV', 10, money)
    worksheet.set_column('AX:AX', 10, percent_format)

    worksheet.set_column(1, 63, 25)  # Width of columns  set to 25

    worksheet.set_column('A:A', None, None, {'hidden': True})
    worksheet.set_column('C:C', None, None, {'hidden': True})
    worksheet.set_column('I:I', None, None, {'hidden': True})
    worksheet.set_column('P:P', None, None, {'hidden': True})
    worksheet.set_column('AG:AG', None, None, {'hidden': True})
    worksheet.set_column('AI:AI', None, None, {'hidden': True})
    worksheet.set_column('AK:AK', None, None, {'hidden': True})
    worksheet.set_column('AM:AM', None, None, {'hidden': True})
    worksheet.set_column('AQ:AQ', None, None, {'hidden': True})
    worksheet.set_column('AR:AR', None, None, {'hidden': True})
    worksheet.set_column('AT:AT', None, None, {'hidden': True})
    worksheet.set_column('BC:BC', None, None, {'hidden': True})
    worksheet.set_column('BE:BE', None, None, {'hidden': True})
    worksheet.set_column('BG:BG', None, None, {'hidden': True})

    worksheet.set_column(0, 63, 25)  # Width of columns B:D set to 30.
    # cell_bg_color = workbook.add_format({'bg_color': "magenta"})
    # worksheet.set_row(0, 15, cell_format=cell_bg_color)  # Set the height of Row 1 to 15.
    writer1.save()

def main_code():
    time = datetime.datetime.now().strftime("%d-%m-%y___%H-%M-%S")
    client = connect_to_MongoDB()
    market_list = pull_input_to_list('INPUT SAMPLE Dallas.csv')  # ---------------> file name here
    print("Pulling Input for property ids")
    df_property_ids = pull_properties_init(client,market_list)
    print("Pulling Property's Loans for Initial Balance")
    df_properties_loans = pull_properties_loans(client,df_property_ids)
    print("Identifying properties by purchase price/Loan value")
    df_relevant_properties = find_relevant_properties(df_property_ids,df_properties_loans)
    print("Pulling Loans Info")
    df_loan_info = extract_loan_info(client, df_relevant_properties["Loan_id"].drop_duplicates(keep="first").dropna().tolist())
    df_relevant_properties_and_loans = merge_two_dataframes_left(df_relevant_properties, df_loan_info,"Loan_id","_id")
    df_relevant_properties_and_loans.sort_values("Loan_id", ascending=False, inplace=True)
    df_relevant_properties_and_loans.drop_duplicates(subset=["Properties_id"], keep="first", inplace=True)
    print("Pulling Properties Info")
    df_properties_full_info = extract_property_info(client,df_relevant_properties_and_loans["Properties_id"].drop_duplicates(keep="first").dropna().apply(ObjectId).tolist())
    df_relevant_properties_and_loans["Properties_id"] = df_relevant_properties_and_loans["Properties_id"].astype(str)
    df_properties_full_info["_id"] = df_properties_full_info["_id"].astype(str)
    df_properties_and_loans_full_info = merge_two_dataframes_left(df_relevant_properties_and_loans, df_properties_full_info, "Properties_id","_id")
    print("Pulling Contacts")
    df_all_including_contacts = pull_contacts(client,df_properties_and_loans_full_info)
    print("Puling some parents info")
    df_final = pull_parents_info(client, df_all_including_contacts)
    print("Creating Excel File")
    format_and_export_to_excel(df_final, time)
    client.close()
main_code()

