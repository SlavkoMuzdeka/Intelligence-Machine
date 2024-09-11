import os

from website_conf_speakers import get_speakers_from_conf_websites

df = get_speakers_from_conf_websites()
df_path = os.path.join(os.getcwd(), "Data", "all_speakers_website.csv")
df.to_csv(df_path, index=False)
