import abc
import argparse
import multiprocessing
import os
import sqlite3
from typing import Optional

from util import copy_files_with_same_name


class DatasetBase(abc.ABC):
    @property
    @abc.abstractmethod
    def tag_column(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def extension_column(self) -> str:
        pass

    @abc.abstractmethod
    def build_query(
        self,
        tags: list[str],
        ng_tags: Optional[list[str]],
        ratings: Optional[list[str]],
        extensions: Optional[list[str]],
        size: Optional[int],
        md5_prefix: Optional[list[str]],
    ) -> str:
        pass

    @abc.abstractmethod
    def get_image_subdir_and_filename(
        self, img_dir: str, id: int, filename: str, extension: str
    ) -> tuple[str, str]:
        pass

    def build_tags_query(self, tags: list[str], ng_tags: Optional[list[str]]) -> str:
        tag_query = " AND ".join([f"{self.tag_column} LIKE '%{tag}%'" for tag in tags])
        if ng_tags is None:
            return tag_query
        ng_tag_query = " AND ".join(
            [f"{self.tag_column} NOT LIKE '%{tag}%'" for tag in ng_tags]
        )
        return f"{tag_query} AND {ng_tag_query}"

    def build_filter_query(
        self,
        ratings: Optional[list[str]],
        extensions: Optional[list[str]],
        size: Optional[int],
        md5_prefix: Optional[list[str]],
    ) -> str:
        if ratings is None:
            ratings_filter = "TRUE"
        else:
            ratings_string = ", ".join([f'"{r}"' for r in ratings])
            ratings_filter = f"rating IN ({ratings_string})"

        if extensions is None:
            extensions_filter = "TRUE"
        else:
            extensions_string = ", ".join([f'"{e}"' for e in extensions])
            extensions_filter = f"{self.extension_column} IN ({extensions_string})"

        if size is None:
            size_filter = "TRUE"
        else:
            size_filter = f"width >= {size} AND height >= {size}"

        if md5_prefix is None:
            md5_filter = "TRUE"
        else:
            md5_filter = " OR ".join([f"md5 LIKE '{m}%'" for m in md5_prefix])

        return f"""
        ({extensions_filter}) AND
        ({ratings_filter}) AND
        ({size_filter}) AND
        ({md5_filter})
        """


class Danbooru(DatasetBase):
    @property
    def tag_column(self) -> str:
        return "tag_string"

    @property
    def extension_column(self) -> str:
        return "file_ext"

    def build_query(
        self,
        tags: list[str],
        ng_tags: Optional[list[str]],
        ratings: Optional[list[str]],
        extensions: Optional[list[str]],
        size: Optional[int],
        md5_prefix: Optional[list[str]],
    ) -> str:
        tags_query = self.build_tags_query(tags, ng_tags)
        filter_query = self.build_filter_query(
            ratings,
            extensions,
            size,
            md5_prefix,
        )
        return f"""
        SELECT id, md5, {self.extension_column}, tags FROM posts
        WHERE {tags_query} AND {filter_query}
        """

    def get_image_subdir_and_filename(
        self, img_dir: str, id: int, filename: str, extension: str
    ) -> tuple[str, str]:
        return filename[:2], f"{filename}.{extension}"


class Gelbooru(DatasetBase):
    @property
    def tag_column(self) -> str:
        return "tags"

    @property
    def extension_column(self) -> str:
        return "extension"

    def __init__(self, category) -> None:
        super().__init__()
        self.category = category

    def build_query(
        self,
        tags: list[str],
        ng_tags: Optional[list[str]],
        rating: Optional[list[str]],
        extensions: Optional[list[str]],
        size: Optional[int],
        md5_prefix: Optional[list[str]],
    ) -> str:
        tags_query = self.build_tags_query(tags, ng_tags)
        filter_query = self.build_filter_query(rating, extensions, size, md5_prefix)
        return f"""
        SELECT id, md5, {self.extension_column}, tags FROM images
        WHERE {tags_query} AND {filter_query}
        """

    def get_image_subdir_and_filename(
        self, img_dir: str, id: int, filename: str, extension: str
    ) -> tuple[str, str]:
        return "", f"{self.category}_{id}_{filename}.{extension}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="Database file of the dataset")
    parser.add_argument("--img-dir", required=True, help="Image directory")
    parser.add_argument("--out-dir", required=True, help="Output directory")
    parser.add_argument(
        "--mode", required=True, help="Dataset mode (danbooru or gallery-dl)"
    )
    parser.add_argument("--category", help="Gallery-dl category name")
    parser.add_argument("--tags", required=True, nargs="+", help="Tags to be included")
    parser.add_argument("--ng-tags", nargs="+", help="Tags to be excluded")
    parser.add_argument("--ratings", nargs="+", help="Ratings")
    parser.add_argument("--extensions", nargs="+", help="Extensions")
    parser.add_argument(
        "--size", type=int, help="Minimum image size (width and height)"
    )
    parser.add_argument("--md5", nargs="+", help="MD5 prefix")
    parser.add_argument(
        "--output-tags", action="store_true", help="Output tags to a text file"
    )
    args = parser.parse_args()

    if args.mode == "danbooru":
        dataset = Danbooru()
    elif args.mode == "gallery-dl":
        if args.category is None:
            raise ValueError("Category name is required for gallery-dl mode")
        # TODO: Support other datasets
        dataset = Gelbooru(args.category)
    else:
        raise ValueError(f"Unknown mode: {args.mode}")

    copy_files(
        dataset,
        args.db,
        args.img_dir,
        args.out_dir,
        args.tags,
        args.ng_tags,
        args.ratings,
        args.extensions,
        args.size,
        args.md5,
        args.output_tags,
    )


def copy_files(
    dataset: DatasetBase,
    db_path: str,
    img_dir: str,
    out_dir: str,
    tags: list[str],
    ng_tags: Optional[list[str]],
    ratings: Optional[list[str]],
    extensions: Optional[list[str]],
    size: Optional[int],
    md5_prefix: Optional[list[str]],
    output_tags: bool,
):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = dataset.build_query(tags, ng_tags, ratings, extensions, size, md5_prefix)
    print(query)
    cursor.execute(query)

    all_files = set(os.listdir(img_dir))
    os.makedirs(out_dir, exist_ok=True)

    total_record_count = 0
    while True:
        records = cursor.fetchmany(10000)
        if not records:
            break
        print(f"Copying {len(records)} images and associated files...")
        total_record_count += len(records)
        with multiprocessing.Pool() as pool:
            pool.starmap(
                copy_file,
                [
                    (
                        dataset,
                        img_dir,
                        out_dir,
                        all_files,
                        id,
                        filename,
                        extension,
                        output_tags,
                        tags_string,
                    )
                    for (id, filename, extension, tags_string) in records
                ],
            )
    conn.close()
    print(f"{total_record_count} images and associated files copied.")


def copy_file(
    dataset: DatasetBase,
    img_dir: str,
    out_dir: str,
    available_files: set[str],
    id: int,
    raw_filename: str,
    extension: str,
    output_tags: bool,
    tags_string: str,
):
    subidr, filename = dataset.get_image_subdir_and_filename(
        img_dir, id, raw_filename, extension
    )
    copy_files_with_same_name(
        filename, os.path.join(img_dir, subidr), out_dir, available_files
    )

    if output_tags:
        filename_without_extension = os.path.splitext(filename)[0]
        with open(f"{out_dir}/{filename_without_extension}.txt", "w") as f:
            f.write(tags_string)


if __name__ == "__main__":
    main()
