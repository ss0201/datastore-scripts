import argparse
import multiprocessing
import os
import shutil


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search for images associated with text files containing specified "
        "captions and copy them to an output directory."
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Directory containing text files associated with images.",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Directory where matched images and text files will be copied to.",
    )
    parser.add_argument(
        "--captions",
        type=str,
        nargs="+",
        required=True,
        help="Captions to search for in the text files. If a text file contains "
        "all of these captions, its associated image and text file will be copied.",
    )
    return parser.parse_args()


def copy_if_contains_captions(
    input_dir: str,
    text_file: str,
    available_files: set[str],
    output_dir: str,
    captions: list[str],
) -> None:
    text_file_path = os.path.join(input_dir, text_file)
    with open(text_file_path, "r") as f:
        captions_in_text_file = f.read()

    if all(c in captions_in_text_file for c in captions):
        shutil.copy2(text_file_path, os.path.join(output_dir, text_file))
        print(f"Copied file {text_file}")

        base_name_without_ext = os.path.splitext(text_file)[0]

        for file in available_files:
            if file.startswith(base_name_without_ext) and file != text_file:
                file_path = os.path.join(input_dir, file)
                shutil.copy2(file_path, os.path.join(output_dir, file))
                print(f"Copied file {file}")


def process_files(input_dir: str, output_dir: str, captions: list[str]) -> None:
    all_files = set(os.listdir(input_dir))

    file_args = [
        (
            input_dir,
            text_file,
            all_files,
            output_dir,
            captions,
        )
        for text_file in all_files
        if text_file.endswith(".txt")
    ]

    with multiprocessing.Pool() as pool:
        pool.starmap(copy_if_contains_captions, file_args)


def main() -> None:
    args = parse_arguments()
    os.makedirs(args.output, exist_ok=True)
    process_files(args.input, args.output, args.captions)


if __name__ == "__main__":
    main()
