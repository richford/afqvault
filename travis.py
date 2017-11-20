import upload
import pandas as pd

df = pd.read_csv("./manifest.csv")

for idx, row in df.iterrows():
    upload.upload_repo(row.username, row.repository_name)
