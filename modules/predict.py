import json
import dill
import pandas as pd
import os
import glob
from datetime import datetime


path = os.environ.get('PROJECT_PATH', '.')

def predict() -> None:

    model_files = glob.glob(f"{path}/data/models/*.pkl")

    model_files.sort(key=os.path.getmtime, reverse=True)

    latest_model_path = model_files[0]

    with open(latest_model_path, 'rb') as f:
        model = dill.load(f)

    test_files = glob.glob(f"{path}/data/test/*.json")

    predictions = []

    for test_file in test_files:
        with open(test_file) as fin:
            form = json.load(fin)
            df = pd.DataFrame.from_dict([form])
            y = model.predict(df)

            predictions.append({
                'file': os.path.basename(test_file),
                'prediction': y[0],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

    predictions_df = pd.DataFrame(predictions)
    predictions_df.to_csv(f"{path}/data/predictions/pred.csv", index=False)


if __name__ == '__main__':
    predict()