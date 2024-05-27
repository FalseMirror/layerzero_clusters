import csv
import json
from datetime import datetime
from loguru import logger
from collections import defaultdict
import concurrent.futures
import pandas as pd

def get_layerzero_ineligible_wallets():
    wallets = []
    with open("data/ineligible_wallets.csv", newline='') as csvfile:
        data = csv.DictReader(csvfile)
        for i, row in enumerate(data):
            if "ADDRESS" in row:
                address = row["ADDRESS"].lower()
                wallets.append(address)
    return wallets

def dump_json_file(result: list | dict, filepath: str):
    with open(filepath, "w") as file:
        json.dump(result, file, indent=4, ensure_ascii=False)

def load_json_file(filepath: str):
    with open(filepath, "r") as file:
        return json.load(file)

def write_results_to_txt_file(results: list | dict, filepath: str):
    with open(filepath, 'w') as file:
        for data in results:
            if data:
                file.write(f'{data}\n')

def process_file_wallets_data(data_path: str, wallets: list | dict):
    try:
        logger.info(f"Processing: {data_path}")
        data = load_json_file(data_path)
        local_results = {wallet: [] for wallet in wallets}
        zero = 0
        for wallet in local_results:
            if wallet in data:
                zero += 1
                for tx in data[wallet]:
                    if tx not in local_results[wallet]:
                        local_results[wallet].append(tx)
        return local_results, zero
    except Exception as e:
        logger.error(f"Error processing {data_path}: {e}")
        return {}, 0

def sort_transactions_by_date(data: dict):
    for wallet, transactions in data.items():
        transactions.sort(key=lambda x: datetime.strptime(x["SOURCE_TIMESTAMP_UTC"], '%Y-%m-%d %H:%M:%S.%f'))
    return data

def find_matching_wallets_by_track(input_file: str, min_length=10, min_wallets=20):
    """
    Find matching wallet routes based on subsequence matches.

    Parameters:
    - min_length: Minimum length of the subsequences to consider.
    - min_wallets: Minimum number of wallets that must share the same subsequence for it to be included in the results.
    """

    df = pd.read_csv(input_file)

    track_data = defaultdict(list)
    for _, row in df.iterrows():
        transactions = [transaction for transaction in row['TRANSACTIONS'].split(',') if transaction]
        if transactions:
            track_data[tuple(transactions)].append(row['SENDER_WALLET'])

    def get_subsequences(route, length):
        return [tuple(route[i:i+length]) for i in range(len(route) - length + 1)]

    logger.info(f"Number of unique tracks: {len(track_data)} | Start matching...")

    all_matching_wallets = []
    for route, wallets in track_data.items():
        subseq_map = defaultdict(list)
        for wallet in wallets:
            for subseq in get_subsequences(route, min_length):
                subseq_map[subseq].append(wallet)

        matching_wallets = set()
        for addresses in subseq_map.values():
            if len(addresses) > 1:
                matching_wallets.update(addresses)

        if len(matching_wallets) >= min_wallets:
            all_matching_wallets.append((route, list(matching_wallets)))

    results = {}
    for route, group in all_matching_wallets:
        if len(group) >= min_wallets:
            results[route] = group

    logger.success(f"Number of tracks: {len(results)}")
    results_str_keys = {",".join(route): wallets for route, wallets in results.items()}

    return results_str_keys

class Sybils():

    def __init__(self) -> None:
        self.address_track = 'data/address_track.csv'
        self.ineligible_wallets = get_layerzero_ineligible_wallets()
        self.max_workers = 5
        self.tx_limit = None
        self.chunk_size = 1_000_000
        self.chunks_amount = 129 # number of chunks of files

        self.min_length = 25 # This parameter specifies the minimum number of wallets to add to the result list
        self.min_wallets = 40


    def get_datas_path(self):
        datas_path = []
        for i in range(int(self.chunks_amount)):
            datas_path.append(f"data/chunks/data_{i+1}.json")
        return datas_path
    
    def save_chunk(self, data: list | dict, filepath: str):
        dump_json_file(data, filepath)
        logger.info(f"Chunk saved to {filepath}")
    
    def get_data(self):
        with open('data/transactions.csv', newline='') as csvfile:
            transactions = csv.DictReader(csvfile)

            logger.debug(f"Creating chunks...")

            results = {}            
            for i, tx in enumerate(transactions):
                try:
                    wallet = tx["SENDER_WALLET"].lower()
                    results.setdefault(wallet, []).append(tx)
                except Exception as error:
                    logger.error(f"{tx} | error: {error}")

                if i % self.chunk_size == 0 and i != 0:
                    logger.info(f"Processed {i} transactions, saving chunk...")
                    id_ = i // self.chunk_size
                    self.save_chunk(results, f"data/chunks/data_{id_}.json")
                    results.clear()

                if self.tx_limit and i >= self.tx_limit - 1:
                    break

            if results:
                self.save_chunk(results, f"results/chunks/data_{id_+1}.json")
            return results

    def create_json_address_track(self):
        logger.debug("Creating json file...")
        results = find_matching_wallets_by_track(self.address_track, min_length=self.min_length, min_wallets=self.min_wallets)
        dump_json_file(results, f"data/track_sybils.json")

    def get_wallets_data(self, wallets: list):
        datas_path = self.get_datas_path()
        wallets = [wallet.lower() for wallet in wallets]

        results = {wallet: [] for wallet in wallets}
        zero_count = 0

        with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(process_file_wallets_data, data_path, wallets) for data_path in datas_path]

            for future in concurrent.futures.as_completed(futures):
                local_results, zero = future.result()
                zero_count += zero
                for wallet, txs in local_results.items():
                    results[wallet].extend(txs)

        wallets_tx_amount = {wallet: len(txs) for wallet, txs in results.items()}

        sorted_data = sort_transactions_by_date(results)
        wallets_amount = sum(1 for wallet in results)
        logger.info(f"Wallets: {wallets_amount}")

        return sorted_data, wallets_tx_amount

    def get_wallet_with_same_tracks(self):
        tracks_sybils = load_json_file("data/track_sybils.json")

        logger.info("Start sort wallets...")
        total_wallets = []
        for track, wallets_list in tracks_sybils.items():
            for wallet in wallets_list:
                total_wallets.append(wallet)

        total_wallets = [wallet.lower() for wallet in total_wallets if wallet not in self.ineligible_wallets]
        logger.success(f"{len(total_wallets)} wallets")

        data_wallets, wallets_tx_amount = self.get_wallets_data(total_wallets)

        results = {}
        sybil_tracks = []
        for track, wallets_list in tracks_sybils.items():
            if track not in results:
                results[track] = {}

            if track not in sybil_tracks:
                sybil_tracks.append(track)

            for wallet in wallets_list:
                if wallet in data_wallets:
                    if wallet not in results[track]:
                        results[track][wallet] = []
                    
                    for tx in data_wallets[wallet]:
                        results[track][wallet].append(tx)

        return results, total_wallets, sybil_tracks


    def get_new_results(self, tracks, sybils):
        results = {}
        track_addresses = {}
        all_wallets = []
        for track, wallets in sybils.items():
            track_wallets = []
            if track not in tracks:
                continue

            for wallet in wallets:
                if len(wallet) == 42:
                    track_wallets.append(wallet)

            if track_wallets and len(track_wallets) >= 50:
                results[track] = sybils[track]
                track_addresses[track] = track_wallets

                for wallet in track_wallets:
                    all_wallets.append(wallet)
        


        tracks_done = [track for track in results]
        logger.success(len(all_wallets))
        return results, all_wallets, tracks_done, track_addresses
    
    def get_tracks_wallets_stats(self, data):

        results = {}
        wallets_tx_amount = {}
        for track, wallets in data.items():
            results[track] = {}
            wallets_tx_amount[track] = {}

            for wallet, txs in wallets.items():
                results[track][wallet] = []
                wallets_tx_amount[track][wallet] = {"tx": 0, "value":0}
                for tx in txs:
                    wallets_tx_amount[track][wallet]["tx"] += 1
                    timestamp = tx["SOURCE_TIMESTAMP_UTC"]
                    value = tx["STARGATE_SWAP_USD"]
                    results[track][wallet].append(
                        {"timestamp": timestamp, "value": value}
                    )
                    if value == "":
                        value = 0
                    else:
                        value = float(value)
                    wallets_tx_amount[track][wallet]["value"] += value

        return results, wallets_tx_amount

    def main(self):

        sybils_all_txs, wallets, sybil_tracks = self.get_wallet_with_same_tracks()

        sybils_wallets_txs, sybils_wallets, sybils_tracks, track_addresses = self.get_new_results(sybil_tracks, sybils_all_txs)
        dump_json_file(track_addresses, f"sybils/track_addresses.json")
        dump_json_file(sybils_wallets_txs, f"sybils/sybils_wallets_txs.json")
        dump_json_file(sybils_tracks, f"sybils/sybils_tracks.json")
        write_results_to_txt_file(sybils_wallets, "sybils/sybils_wallets.txt")

        tracks_wallets_txs_stats, tracks_wallets_total_stats = self.get_tracks_wallets_stats(sybils_wallets_txs)
        dump_json_file(tracks_wallets_txs_stats, f"sybils/tracks_wallets_txs_stats.json")
        dump_json_file(tracks_wallets_total_stats, f"sybils/tracks_wallets_total_stats.json")


if __name__ == "__main__":
    sybils = Sybils()
    sybils.get_data() # Create chunks in data/chunks
    sybils.create_json_address_track() # Create json file address-track (track_sybils.json)
    sybils.main()
