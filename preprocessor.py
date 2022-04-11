
import sys
import toml
import os
import pandas as pd

from pathlib import Path

class Preprocessor:
    def __init__(self, config):
        self.ou_names = [
            "OU0 - Broadcast",
            "OU0 - ROUTE",
            "OU1 - Generate Plan",
            "OU2 - Initialize Thread",
            "OU3 - Acquire Locks",
            "OU4 - Read from Local",
            "OU5M - Read from Remote",
            "OU6 - Execute Arithmetic Logic",
            "OU7 - Write to Local",
            "OU8 - Commit"
        ]

        self.non_array_columns = [
            "Tx Type",
            "Dependency - Max Depth",
            "Dependency - First Layer Tx Count",
            "Dependency - Total Tx Count",
            "Number of Insert Records",
            "Number of Overflows in Fusion Table"
        ]

        self.array_columns = [
            "Read Data Distribution",
            "Read Data in Cache Distribution",
            "Update Data Distribution",
            "System CPU Load",
            "Process CPU Load",
            "System Load Average",
            "Thread Active Count",
            "I/O Read Bytes",
            "I/O Write Bytes",
            "I/O Queue Length",
            "Number of Read Record in Last 100 us",
            "Number of Read Record Excluding Cache in Last 100 us",
            "Number of Update Record in Last 100 us",
            "Number of Insert Record in Last 100 us",
            "Number of Commit Tx in Last 100 us",
            "Number of Read Record in Last 500 us",
            "Number of Read Record Excluding Cache in Last 500 us",
            "Number of Update Record in Last 500 us",
            "Number of Insert Record in Last 500 us",
            "Number of Commit Tx in Last 500 us",
            "Number of Read Record in Last 1000 us",
            "Number of Read Record Excluding Cache in Last 1000 us",
            "Number of Update Record in Last 1000 us",
            "Number of Insert Record in Last 1000 us",
            "Number of Commit Tx in Last 1000 us"
        ]
        self.feature_cols = self.non_array_columns + self.array_columns

        # Configurations
        self.server_num = config["global"]["server_num"]
        self.warmup_time = config["preprocessor"]["warmup_time"]

    def preprocess(self, input_dir, output_dir):
        df_features = self._load_features(input_dir)

        for server_id in range(self.server_num):
            df_server_features = self._separate_features(df_features, server_id)
            df_server_latencies = self._load_latencies(input_dir, server_id)
            df_server_joined = self._join_data(df_server_features, df_server_latencies, server_id)
            self._save_joined_data(output_dir, df_server_joined, server_id)

    def _load_features(self, input_dir):
        print("Loading features ... ", end = "")

        feature_file_path = os.path.join(input_dir, "transaction-features.csv")
        df_features = read_csv_with_multi_headers(feature_file_path)
        df_features = df_features.set_index("Transaction ID")
        df_features["Tx Type"] = df_features["Tx Type"].astype("int64")

        print("completed")

        return df_features

    def _separate_features(self, df_features, server_id):
        print(f"Separating features for server {server_id} ... ", end = "")

        deep_copied_features = df_features.copy()

        for col in self.array_columns:
            deep_copied_features[col] = deep_copied_features[col].apply(
                convert_array_col, args=(server_id, ))
        
        print("completed")

        return deep_copied_features

    def _load_latencies(self, input_dir, server_id):
        print(f"Loading latencies for server {server_id} ... ", end = "")
        
        latencies_file_path = os.path.join(input_dir,
                f"transaction-latency-server-{server_id}.csv")
        df_latencies = read_csv_with_multi_headers(latencies_file_path)
        df_latencies = df_latencies.set_index("Transaction ID").sort_index()

        print("completed")

        return df_latencies

    def _join_data(self, df_features, df_latencies, server_id):
        print(f"Joining features and leatencies data for server {server_id} ... ", end = "")
        
        df_join = df_features.join(df_latencies, how="inner")
        df_join = df_join[df_join['Is Master'] == True]
        df_join = df_join[df_join["Start Time"] > self.warmup_time]
        
        print("completed")
        
        return df_join
    
    def _save_joined_data(self, output_dir, df_joined_data, server_id):
        print(f"Saving training data for {server_id} ... ", end = "")
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        feature_save_path = os.path.join(output_dir, f"server-{server_id}-features.csv")
        pd.DataFrame(df_joined_data[self.feature_cols]).to_csv(feature_save_path)
        label_save_path = os.path.join(output_dir, f"server-{server_id}-labels.csv")
        pd.DataFrame(df_joined_data[self.ou_names]).to_csv(label_save_path)
        
        print("completed")

def read_csv_with_multi_headers(path):
    with open(path, "r", encoding="utf8") as f:
        lines = f.readlines()

    header_pos = []
    for idx, item in enumerate(lines):
        if "Transaction ID" in item:
            header_pos.append(idx)

    return pd.read_csv(path, skiprows=header_pos[-1])

def convert_array_col(x, server_id):
    x_split = x.split(",")
    return x_split[server_id].replace("[", "").replace("]", "")

# Read arguments
if len(sys.argv) < 4:
    print(f"python {sys.argv[0]} [Config File] [Input Dir] [Output Dir]")
    exit(1)

config_path = sys.argv[1]
input_dir = sys.argv[2]
output_dir = sys.argv[3]

# Read the config file
print(f"config path: {config_path}")
config = toml.load(config_path)

Preprocessor(config).preprocess(input_dir, output_dir)
