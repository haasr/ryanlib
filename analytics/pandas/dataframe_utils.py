import numpy as np
import pandas as pd


def print_title(title):
    print(f"{title}", end="\n"+"-"*44+"\n")


def get_columns_index(df, column_types='all'):
    if column_types == 'all':
        return df.columns
    else:
        numeric_cols = df.select_dtypes(include=np.number).columns
        if column_types == 'numeric':
            return numeric_cols
        elif column_types == 'non-numeric':
            return pd.Index(set(df.columns).difference(set(numeric_cols)))
        else:
            return df.select_dtypes(include=column_types).columns


def pretty_print_columns(cols_index):
    names = list(cols_index)
    [ print(f'{_+1}. '.ljust(5, ' '), f'{name}') for _, name in enumerate(names) ]


def print_duplicates_and_unique(df):
    # check if duplicates
    duplicates_count = 0
    okay_count = 0
    for row in df.duplicated():
        if row:
            duplicates_count += 1
        else:
            okay_count +=1

    print_title("Number of duplicate rows")
    print(duplicates_count)

    print_title("Number of unique rows")
    print(okay_count)


def print_missing_per_col(df, column_types='all'):
    def _print_missing_col(df, cols):
        print(f"Percent missing by column ({column_types})".center(32))
        for col in cols:
            p_missing = round(np.mean(df[col].isnull()), 5)
            print(f'{col} '.ljust(34, '.'), f'{p_missing*100}%')

    _print_missing_col(df, get_columns_index(df, column_types))

# Identify values that are effectively missing but not identified by
# Pandas as such:
def identify_alt_missing(df, column_types='all', search_list=['na', 'nan', 'n/a', 'none'], case_sensitive=False):
    columns_with_missing = {}

    if case_sensitive:
        def _contains_missing(values):
            missing = False
            for val in values:
                val = val.lower()
                if val in search_list:
                    missing = True
                    break
            return missing
    else:
        def _contains_missing(values):
            missing = False
            for val in values:
                if val in search_list:
                    missing = True
                    break
            return missing

    for col in get_columns_index(column_types):
        if _contains_missing(df[col].unique()):
            columns_with_missing[col] = df[col].dtype

    return columns_with_missing


def replace_alt_missing(df, column_types='all', search_list=['na', 'n/a', 'none'],
                                case_sensitive=False, replace_value=np.nan):
    cols_with_missing = identify_alt_missing(df, column_types, search_list, case_sensitive)

    for col in cols_with_missing.keys():
        for item in search_list:
            df.replace({ item: replace_value }, inplace=True)


def convert_columns_data_type(df, cols, datatype):
    for col in cols:
        df[col] = df[col].astype(datatype)


def drop_numeric_outliers(dataframe, mean_multiplier=1, stddevs=3):
    print(f"Rows prior: {dataframe.shape[0]}")

    outlier_thresholds = dataframe.mean(numeric_only=True) # Store the means per column
    print('\nMeans:')
    print(outlier_thresholds) # Print the means becfore I transform them into the thresholds

    if stddevs > 0:
        standard_deviations = dataframe.std(numeric_only=True) # Store the stddevs per column
        print('\nStandard Deviations:')
        print(standard_deviations) # Print the stddevs

        for _ in range(0, len(outlier_thresholds)):
            outlier_thresholds[_] = (mean_multiplier*outlier_thresholds[_]) + (stddevs*standard_deviations[_]) # Make each threshold n stddevs from the column mean
    else:
        for _ in range(0, len(outlier_thresholds)):
            outlier_thresholds[_] = mean_multiplier*outlier_thresholds[_] # Make each threshold n*mean

    print('\nOutlier Thresholds:') # Print the thresholds
    print(outlier_thresholds)

    print('\nDropping...')
    # List comprehension to drop the rows that have a value that exceeds its respective column threshold:
    [ dataframe.drop(dataframe[abs(dataframe[col]) > outlier_thresholds[_]].index, inplace=True)
         for _, col in enumerate(dataframe.select_dtypes(include=np.number).columns) ]

    print(f"Rows after: {dataframe.shape[0]}")


def impute_numeric(dataframe, boolean_cond=None, impute_value='median'):
    counter = 0
    numeric_cols = dataframe.select_dtypes(include=np.number).columns

    if impute_value == 'median':
        def _impute(col):
            m = dataframe[col].median()
            print(f'> Imputing with median {m}')
            dataframe[col].fillna(value=m, inplace=True)
    elif impute_value == 'mean':
        def _impute(col):
            m = dataframe[col].mean()
            print(f'> Imputing with mean {m}')
            dataframe[col].fillna(value=m, inplace=True)
    elif impute_value == 'mode':
        def _impute(col):
            m = dataframe[col].mode()[0]
            print(f'> Imputing with mode {m}')
            dataframe[col].fillna(value=m, inplace=True)
    else:
        def _impute(col):
            print(f'> Imputing with {impute_value}')
            dataframe[col].fillna(value=impute_value, inplace=True)

    if boolean_cond == None:
        def _eval_boolean_cond(col):
            p_missing = round((np.mean(dataframe[col].isnull())*100), 5)
            print(f"{col}: ".ljust(28), f"{p_missing}% missing")
            return p_missing > 0
    else:
        def _eval_boolean_cond(col):
            p_missing = round((np.mean(dataframe[col].isnull())*100), 5)
            print(f"{col}: ".ljust(28), f"{p_missing}% missing")
            return (p_missing > 0 and eval(f"{p_missing}{boolean_cond}"))

    for col in numeric_cols:
        if _eval_boolean_cond(col):
            _impute(col)
            counter += 1

    return f"{counter} numeric columns imputed"


# Modified from an earlier function so I can impute based on no "missing" data but a specific value.

def impute_non_numeric(dataframe, boolean_cond, impute_value='NA', check_missing=True):
    counter = 0
    non_numeric_cols = pd.Index(set(dataframe.columns).difference(set(dataframe.select_dtypes(include=(np.number)).columns)))
    impute_val = impute_value

    for col in non_numeric_cols:
        if impute_value == 'most_common':
            impute_val = dataframe[col].mode()[0] # Take the first value -- since these are non-numeric, this is fine

        if check_missing:
            p_missing = round((np.mean(dataframe[col].isnull())*100), 5)
            print(f"{col}: ".ljust(28), f"{p_missing}%")
            impute = p_missing > 0 and eval(f"{p_missing}{boolean_cond}")
        else:
            impute = eval(f"{boolean_cond}")

        if impute:
            counter += 1
            print(f"Imputing with {impute_val}")
            dataframe[col] = dataframe[col].fillna(impute_val)

    return f"{counter} non-numeric columns imputed"

# My version of a describe function
def desc(df):
    numeric_df = df.select_dtypes(include=np.number)

    # Create Series objects so I can merge them all together at the end:
    mean = numeric_df.mean()
    mode = numeric_df.mode()
    median = numeric_df.median()
    n_range = numeric_df.max() - numeric_df.min()
    iqr = numeric_df.quantile(0.75) - numeric_df.quantile(0.25) # I prefer this to get them all at once
    mode = pd.Series(mode.values.flatten(), index=mean.index) # Original Series does not have the right index. Borrowing from the mean

    # Make a nice table to display all the stats:
    desc_stats = pd.concat([mean, mode, median, n_range, iqr], axis=1)
    desc_stats.columns = ('Mean', 'Mode', 'Median', 'Range', 'IQR') # Set manually so I don't display 0, 1, 2..4
    return desc_stats