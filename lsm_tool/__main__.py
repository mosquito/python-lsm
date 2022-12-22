from argparse import ArgumentParser
from pathlib import Path


def main():
    parser = ArgumentParser(
        description=(
            "This utility manipulates an LSM database content. using one of the "
            "compression algorithms, or copies the database without "
            "compression, or simply copies the database element by element. "
            "You can also use it to verify the integrity of the database."
        )
    )
    parser.add_argument(
        "input", type=Path, help="Source LSM database file path"
    )
    parser.add_argument(
        "output", type=Path, help="Destination LSM database file path"
    )
