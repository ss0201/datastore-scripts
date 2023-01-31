import glob
import multiprocessing
import os
import sqlite3


def delete_file(md5: str, rating: str) -> None:
    image_path = f"images/{md5[:2]}/{md5}"
    for path in glob.glob(f"{image_path}*"):
        print(path, rating)
        os.remove(path)


conn = sqlite3.connect("danbooru.sqlite")
cursor = conn.cursor()
cursor.execute("SELECT md5, rating FROM posts WHERE rating IN ('g', 's')")

while True:
    records = cursor.fetchmany(10000)

    if not records:
        break

    with multiprocessing.Pool() as pool:
        pool.starmap(delete_file, records)

cursor.execute("DELETE FROM posts WHERE rating IN ('g', 's')")

conn.commit()
