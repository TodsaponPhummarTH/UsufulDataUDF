import os
import pandas as pd

def read_parquet_and_save(parquet_file_path, output_csv_path=None):
    """
    Reads a Parquet file and either saves it to a CSV file or returns a Pandas DataFrame.

    :param parquet_file_path: str, path to the input Parquet file
    :param output_csv_path: str, path to the output CSV file (optional)
    :return: pd.DataFrame if output_csv_path is None, otherwise None
    """
    # Check if the Parquet file exists
    if not os.path.exists(parquet_file_path):
        print(f"Error: The file {parquet_file_path} does not exist.")
        return

    # Read the Parquet file into a DataFrame
    df = pd.read_parquet(parquet_file_path)

    # Display the first 10 rows of the DataFrame
    print("Sample of the data (first 10 rows):")
    print(df.head(10))

    if output_csv_path:
        # Save the DataFrame to a CSV file
        df.to_csv(output_csv_path, index=False)
        print(f"Data saved to {output_csv_path}")
    else:
        # Return the DataFrame
        return df

# INPUT HERE
parquet_file_path = r"C:\Users\Todsapon\Downloads\redshift-rbac-rls-policy-serverless\redshift-rbac-rls-policy-serverless\redshift-rbac-rls-policy-serverless\repo=ecu-dist\revision=1\b407c8f68a964c01864f5b8e5a16af7e.snappy.parquet"
output_csv_path = r"C:\Users\Todsapon\Downloads\redshift-rbac-rls-policy-serverless\redshift-rbac-rls-policy-serverless\redshift-rbac-rls-policy-serverless\repo=ecu-dist\revision=1\b407c8f68a964c01864f5b8e5a16af7e.csv"

print(f"Reading Parquet file from: {parquet_file_path}")
print(f"Saving CSV file to: {output_csv_path}")

read_parquet_and_save(parquet_file_path, output_csv_path)