import argparse
import os


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run_counts", type=int, help="How many times to run the model?"
    )
    args = parser.parse_args()

    if args.run_counts is None:
        print("Please provide the --run_counts argument.")
    else:
        run_counts = args.run_counts

        for i in range(run_counts):
            os.system("python baus.py --mode simulation --disable-slack --random-seed")
