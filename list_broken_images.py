import argparse
import concurrent.futures
import os
from typing import Optional

from PIL import Image


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("img_dir", type=str)

    args = parser.parse_args()
    img_dir = args.img_dir

    image_paths = get_image_paths(img_dir)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(get_image_error, image_paths))

    for image_path, error in zip(image_paths, results):
        if error:
            print(image_path, error)


def get_image_paths(directory: str) -> list:
    extensions = (".jpg", ".png")
    image_paths = []

    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(extensions):
                image_paths.append(os.path.join(root, file))
    return image_paths


def get_image_error(image_path: str) -> Optional[Exception]:
    try:
        with Image.open(image_path) as img:
            img.verify()
        return None
    except IOError as e:
        return e


if __name__ == "__main__":
    main()
