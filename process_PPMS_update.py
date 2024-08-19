

import pandas
import matplotlib.pyplot as plt


def main():
    df = pandas.read_csv(f'PPMS_update.csv', header=None)
    
    # Database has 29,318,957 rows
    # This file has 
    print(len(df))
    
    columns = [
        'transaction_unique_id',
        'price',
        'transaction_date',
        'postcode',
        'property_type',
        'new_tag',
        'lease',
        'primary_address_object_name',
        'secondary_address_object_name',
        'street',
        'locality',
        'town_city',
        'district',
        'county',
        'ppd_cat',
        'record_status',
        'file_date',
    ]
    df.columns = columns
    print(df)
    
    df['file_date'] = pandas.to_datetime(df['file_date'])
    df['transaction_date'] = pandas.to_datetime(df['transaction_date'])
    df['file_date_transaction_date_diff'] = df['file_date'] - df['transaction_date']
    df['file_date_transaction_date_diff_days'] = df['file_date_transaction_date_diff'].dt.days
    df['file_date_transaction_date_diff_years'] = (1.0 / 365.25) * df['file_date_transaction_date_diff_days']
    
    print(df.loc[df['file_date_transaction_date_diff_days'] < 0])
    df = df.loc[df['file_date_transaction_date_diff_days'] > 0]
    
    fig, ax = plt.subplots(1, 1)
    
    ax.hist(df['file_date_transaction_date_diff_years'], bins=250)
    ax.set_yscale('log')
    
    fig.savefig(f'file_date_transaction_date_diff_years.png')
    fig.savefig(f'file_date_transaction_date_diff_years.pdf')
    
    df = df.loc[df['file_date_transaction_date_diff_years'] < 5.0]
    
    fig, ax = plt.subplots(1, 1)
    
    ax.hist(df['file_date_transaction_date_diff_years'], bins=250)
    ax.set_yscale('log')
    
    fig.savefig(f'file_date_transaction_date_diff_years_lt5.png')
    fig.savefig(f'file_date_transaction_date_diff_years_lt5.pdf')
    
    df = df.loc[df['file_date_transaction_date_diff_days'] <= 365]
    
    fig, ax = plt.subplots(1, 1)
    
    ax.hist(df['file_date_transaction_date_diff_days'], bins=365)
    ax.set_yscale('log')
    
    fig.savefig(f'file_date_transaction_date_diff_years_lt1.png')
    fig.savefig(f'file_date_transaction_date_diff_years_lt1.pdf')


if __name__ == '__main__':
    main()
    
    