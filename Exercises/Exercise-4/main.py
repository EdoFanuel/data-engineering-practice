import os
import json
import pandas as pd

def main():
    data = []
    for root, _, files in os.walk('data'):
        for file in files:
            filepath = f'{root}/{file}'
            print(f'Processing {filepath}')
            try:
                data.append(parse_json(filepath))
            except:
                pass
    df = pd.json_normalize(data)
    df.to_csv('answer.csv', index=False)


def parse_json(filepath):
    with open(filepath) as json_file:
        data = json.load(json_file)
        return data

if __name__ == "__main__":
    main()
