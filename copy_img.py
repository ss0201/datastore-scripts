import abc
import argparse
import multiprocessing
import os
import shutil
import sqlite3


class DatasetBase(abc.ABC):
    @abc.abstractmethod
    def get_query(self, tag: str) -> str:
        pass

    @abc.abstractmethod
    def get_image_path(
        self, img_dir: str, id: int, filename: str, extension: str
    ) -> str:
        pass


class Danbooru(DatasetBase):
    def get_query(self, tag: str) -> str:
        return f"SELECT id, md5, file_ext FROM posts WHERE tag_string LIKE '%{tag}%'"

    def get_image_path(
        self, img_dir: str, id: int, filename: str, extension: str
    ) -> str:
        return f"{img_dir}/{filename[:2]}/{filename}.{extension}"


class GalleryDl(DatasetBase):
    def __init__(self, category) -> None:
        super().__init__()
        self.category = category

    def get_query(self, tag: str) -> str:
        return f"SELECT id, filename, extension FROM images WHERE tags LIKE '%{tag}%'"

    def get_image_path(
        self, img_dir: str, id: int, filename: str, extension: str
    ) -> str:
        return f"{img_dir}/{self.category}_{id}_{filename}.{extension}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m", "--mode", required=True, help="Dataset mode (danbooru or gallery-dl)"
    )
    parser.add_argument(
        "-t", "--tag", required=True, help="Tag to be used to filter the files"
    )
    parser.add_argument(
        "-d", "--db", required=True, help="Database file of the dataset"
    )
    parser.add_argument("-i", "--img", required=True, help="Image directory")
    parser.add_argument("-o", "--out", required=True, help="Output directory")
    parser.add_argument("-c", "--category", help="Gallery-dl category name")
    args = parser.parse_args()

    if args.mode == "danbooru":
        dataset = Danbooru()
    elif args.mode == "gallery-dl":
        if args.category is None:
            raise ValueError("Category name is required for gallery-dl mode")
        dataset = GalleryDl(args.category)
    else:
        raise ValueError(f"Unknown mode: {args.mode}")

    copy_files(dataset, args.tag, args.db, args.img, args.out)


def copy_files(
    dataset: DatasetBase, tag: str, db_path: str, img_dir: str, out_dir: str
):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(dataset.get_query(tag))
    while True:
        records = cursor.fetchmany(10000)
        if not records:
            break
        print(f"Copying {len(records)} files...")
        with multiprocessing.Pool() as pool:
            pool.starmap(
                copy_file,
                [
                    (dataset, img_dir, id, out_dir, filename, extension)
                    for (id, filename, extension) in records
                ],
            )
    conn.close()
    print(f"{len(records)} files copied.")


def copy_file(
    dataset: DatasetBase,
    img_dir: str,
    id: int,
    out_dir: str,
    filename: str,
    extension: str,
):
    src_path = dataset.get_image_path(img_dir, id, filename, extension)
    print(src_path)
    os.makedirs(out_dir, exist_ok=True)
    shutil.copy2(src_path, out_dir)


if __name__ == "__main__":
    main()
