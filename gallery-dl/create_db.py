import argparse
import glob
import json
import os
import sqlite3


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--img", required=True, help="gallery-dl image directory")
    parser.add_argument("-o", "--out", required=True, help="Output db file")
    args = parser.parse_args()

    register_files(args.img, args.out)


def register_files(img_dir: str, db_file: str) -> None:
    json_files = glob.glob(f"{img_dir}/*.json")
    db_dir = os.path.dirname(db_file)
    os.makedirs(db_dir, exist_ok=True)
    db_conn = create_db(db_file)
    try:
        cursor = db_conn.cursor()
        for src_json_path in json_files:
            image_file_path = os.path.splitext(src_json_path)[0]
            if not os.path.exists(image_file_path):
                continue
            register_file(src_json_path, cursor)
        db_conn.commit()
    except Exception as e:
        print(e)
        db_conn.rollback()
    finally:
        db_conn.close()


def create_db(db_file: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS images (
            id INTEGER PRIMARY KEY,
            created_at DATETIME NOT NULL,
            filename TEXT NOT NULL,
            extension TEXT NOT NULL,
            md5 TEXT NOT NULL,
            width INTEGER NOT NULL,
            height INTEGER NOT NULL,
            rating TEXT NOT NULL,
            tags TEXT NOT NULL
        );
        """
    )
    conn.commit()
    return conn


def register_file(src_json_path: str, cursor: sqlite3.Cursor) -> None:
    with open(src_json_path, "r") as f:
        data = json.load(f)
        cursor.execute(
            """
            INSERT INTO images (id, created_at, filename, extension, md5, width, height, rating, tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["id"],
                data["created_at"],
                data["filename"],
                data["extension"],
                data["md5"],
                data["width"],
                data["height"],
                data["rating"],
                data["tags"],
            ),
        )


if __name__ == "__main__":
    main()
