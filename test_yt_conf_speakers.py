# LOAD AND DOWNLOAD SPEAKERS FROM YT
import os
from yt_conf_speakers import get_speakers_from_yt_playlists

df = get_speakers_from_yt_playlists()
df_path = os.path.join(os.getcwd(), "Data", "all_speakers_yt.csv")
df.to_csv(df_path, index=False)


# import pandas as pd
# import streamlit as st

# st.write(pd.read_csv("Data/all_speakers_yt.csv"))
