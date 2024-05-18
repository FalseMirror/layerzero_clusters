Using this code, I identified 210 clusters and 26.946 sybil wallets. My report is LayerZero Labs Sybil Report Issue.

## Preparation
1. Move the CSV file with all transactions into the data folder and name it transactions.csv.
2. List all possible routes in data/address_track.csv (GitHub does not allow uploading files larger than 25MB).
3. Insert all sybil wallets from LayerZero into data/ineligible_wallets.csv (GitHub does not allow uploading files larger than 25MB).


## Execution
1. Run the main.py file.
2. First, the script will process all transactions in transactions.csv and create JSON files with all transactions sorted by wallets in the data/chunks folder.
3. Then, the script creates a JSON file with all possible routes used by sybils. Each route was used by more than 50 wallets.
4. After that, the script performs all sorts of sorting and gathers statistics, saving them in the sybils folder.

## Results
As a result, 210 clusters with 26.946 sybil wallets will be found.


