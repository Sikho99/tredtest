import os

if __name__ == "__main__":
    os.system("streamlit run main_app.py --server.port=8501 &")
    os.system("streamlit run symbol_app.py --server.port=8502")
