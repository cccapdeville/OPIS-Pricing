# -*- coding: utf-8 -*-
"""
Created on Thu Oct 14 19:49:17 2021

@author: cccap
"""

# setup
import pandas as pd
import regex as re
from collections import defaultdict



# import data
"""
=== User setup: ===
    - working directory
    - import filename
    - filename to save transformed df as

"""

# run the following code to set up filepaths and load data
initial_df_filename='Reno Vegas SanDiego NetClose.xlsx'
transformed_df_filename='Reno_Vegas_SanDiego_NetClose_Flat_File.csv'
raw_data = pd.read_excel(initial_df_filename, header=None)


"""
=== regular expression setup  ===

"""

# the following regex (regular expression) parses OPIS rack price data headers
parse_header = re.compile(r'''^
                           (?P<product>.+)
                           \n
                           (?P<city>.+)\s
                           (?P<state>[A-Z]{2})
                           \n
                           (?P<gross_net>Gross|Net)
                           \n
                           (?P<contract>Contract)?
                           \n?
                           ((?P<branded_unbranded>Branded|Unbranded)|(?P<party>.+?))?                               # the ? after .+ makes this non-greedy, consuming as LITTLE as possible to match the expression
                           \s?
                           (?P<branded_unbranded>u|b|Unbranded|Branded)?
                           \s?
                           (?P<price_type>Low|High|Average)?
                           \s?
                           (?P<CAR>with\sCAR|without\sCAR)?
                           \s?
                           \(
                           (?<=\()
                           (?P<units>.+)
                           (?=\))

                        ''',
                        re.VERBOSE)

test = b['measure'][2]
test
parse_header.findall(test)

# Set new col_names 
# do not change this list without changing the regex group names to match
parsed_col_names = ['product',   
                    'city', 
                    'state', 
                    'gross_net',
                    'contract',
                    'party', 
                    'CAR',
                    'branded_unbranded', 
                    'units', 
                    'price_type'
                    ]

"""
=== Functions ===

"""


def prep_opis(df):
    """ 
    preps an Opis sheet, by removing blank columns 
    and assigning correct headers 
    """
    df = df.iloc[1: , :]
    new_header = df.iloc[0] #grab the first row for the header
    df = df[1:] #take the data less the header row
    df.columns = new_header #set the header row as the df header
    return(df)


def melt_opis(df, var_name="measure", value_name="value"):
   """
   melt the Opis columns into one field (except date and DOW)
   this function assumes data and DOW are in the first two columns
   """
   return(df.melt(id_vars=df.columns[:2], value_vars=a.columns[2:],
        var_name='measure', value_name='value'))


def parse_opis(s):
    """
    this function takes a string, splits and parses it
    rules are designed to treat column headers in Opis pricing sheets
    """
    # for storing parsed text. Key=measure name, value=value
    split_dict = defaultdict(str) 
    for name in parsed_col_names:
        split_dict[name] = 'N/A'

    e = parse_header.groupindex
    for i in split_dict:
        if i in e:
            split_dict[i] = parse_header.match(s).group(i)
 
    return(split_dict)


def cast_new_cols(df, col_names):
    """
    takes a df, then casts new column values based on the 'measure' field
    of a melted opis sheet. 
    Returns a new df
    """
    # new dict d, for storing and writing parsed values to new df
    d = defaultdict(list)
    
    # for each header, parse it, then save the returned value in dict d
    df2 = df.copy()
    for i in df2['measure']:
        parsed = parse_opis(i)
        for key in parsed:
            d[key].append(parsed.get(key))
    
    # for each key in dict d, assign the values to a new column in the new df
    for key in d:
        df2[key] = d.get(key)

    return(df2)


"""
=== execute the functions ===

"""

# runs the below sequence of functions                  
a = prep_opis(raw_data)
b = melt_opis(a)
c = cast_new_cols(b, parsed_col_names)

# remove all rows where value is blank
c['value'].isnull().sum() # = a lot
c.dropna(subset=['value'], inplace=True)
c['value'].isnull().sum() # = zero

# create a function to apply to the 'branded_unbranded' column
# to change all u to Unbranded and b to Branded
def col_calc(x):
    if x == 'u' :
        x = 'Unbranded'
    elif x == 'b':
        x = 'Branded'
    return x    


# replace column values with new calculated values (ie convert 'u' to 'Unbranded')
c['branded_unbranded'] = c['branded_unbranded'].apply(lambda x: col_calc(x))

c.columns
c.dtypes
unique_vals = c['measure'].unique()

# write finished file to new csv
c.to_csv(transformed_df_filename)


"""
===COMBINE ALL WORKBOOKS===

"""
file1='Reno_Vegas_SanDiego_GrossClose_Flat_File.csv'
file2='Reno_Vegas_SanDiego_NetClose_Flat_File.csv'
file3='Sparks_Vegas_SanDiego_Cnt_Gross_Flat_File.csv'
file4='Sparks_Vegas_SanDiego_Cnt_Net_Flat_File.csv'

df1 = pd.read_csv(file1, index_col=0)
df2 = pd.read_csv(file2, index_col=0)
df3 = pd.read_csv(file3, index_col=0)
df4 = pd.read_csv(file4, index_col=0)

df_all = pd.concat([df1, df2, df3, df4])

# write finished file to new csv
df_all.to_csv('OPIS_Combined_Flat_file.csv')
