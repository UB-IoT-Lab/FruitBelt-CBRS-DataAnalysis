import os
import pickle as pkl
import pandas as pd

def pkl_to_csv(pkl_path, csv_path):
    with open(pkl_path, "rb") as f:
        data = pkl.load(f)
    df = pd.DataFrame(data)
    df = df.T
    df.to_csv(csv_path, index=False)

def intelligent_avg(row, columns):
    values = [row[col] for col in columns if row[col] != 0 and not pd.isna(row[col])]
    return sum(values) / len(values) if len(values) > 0 else float('nan')

def main(folder_path, output_csv):
    dfs = []

    for file in os.listdir(folder_path):
        if file.endswith('.pkl'):
            pkl_path = os.path.join(folder_path, file)
            temp_csv = os.path.join(folder_path, f"{os.path.splitext(file)[0]}.csv")
            pkl_to_csv(pkl_path, temp_csv)

            temp_df = pd.read_csv(temp_csv)
            dfs.append(temp_df)

            os.remove(temp_csv)

    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df = combined_df.drop_duplicates()
    
    rsrp_columns = ["rsrp0", "rsrp1", "rsrp2", "rsrp3"]
    combined_df["mean_AvgRsrp"] = combined_df.apply(lambda row: intelligent_avg(row, rsrp_columns), axis=1)

    # Remove rows with empty values in the "rsrp0" column
    combined_df = combined_df.dropna(subset=["rsrp0"])

    combined_df.to_csv(output_csv, index=False)



if __name__ == "__main__":
    folder_path = "/path/to/pkl_files"
    output_csv = "/path/to/output/combined.csv"
    main(folder_path, output_csv)
