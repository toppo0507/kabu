import sys
import argparse
from src import download_chart as dc
from src import annalise as an
from src import visualization as vi

parser = argparse.ArgumentParser(description="株式の解析ツール")
parser.add_argument("--mode", type=str, help="[download, annalise, visualization]")
args = parser.parse_args()

def main():
    if args.mode == "download":
        dc.DownloadDataTable().main()
    elif args.mode == "annalise":
        an.Annalise().main()
    elif args.mode == "visualization":
        vi.Visualization().main()





if __name__ == "__main__":
    main()
