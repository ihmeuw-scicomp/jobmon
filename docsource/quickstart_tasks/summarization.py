import os
import argparse
import getpass
from typing import Dict


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--root_data_dir',
                        required=False,
                        action='store',
                        help='root_data_dir')
    parser.add_argument('-v', '--log_level',
                        required=False,
                        action='store',
                        help='log_level')
    args = parser.parse_args()
    return args


def write_dummy_data(args: Dict) -> None:
    output_path = f'{args.root_data_dir}/jobmon_quickstart_example/'
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    with open(f'{output_path}/summaries.txt', 'w') as f:
        f.write(f'summaries, log_level: {args.log_level}')


def main():
    args = get_args()
    write_dummy_data(args)


if __name__ == '__main__':
    main()
