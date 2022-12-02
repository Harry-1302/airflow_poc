from faker import Faker
import pandas as pd
import json
fake = Faker()


def generate_data(x):

    # dict for json creation
    df = {}
    df_temp = []
    
    # pandas dataframe for making csv file
    df_pd = pd.DataFrame()

    for i in range(0, x):

        location_source = list(fake.location_on_land())
        date = fake.date()

        df["advertising_id"] = fake.pystr()
        df["city"] = location_source[2]
        df["location_category"] = location_source[4]
        df["location_granularities"] = fake.street_name()
        df["location_source"] = location_source[2:]
        df["state"] = location_source[-1].split('/')[1]
        df["timestamp"] = fake.unix_time()
        df["user_id"] = fake.pystr()
        df["user_latitude"] = location_source[0]
        df["user_longitude"] = location_source[1]
        df["month"] = date.split('-')[1]
        df["date"] = date

        df_copy = df.copy()
        df_temp.append(df_copy)

    df_pd = pd.DataFrame(df_temp)
    print(df_pd.to_string())

    df_pd.to_csv('dataset.csv')

def main():
    generate_data(50)

main()
