import csv
import json
import os
from pathlib import Path
from typing import Generator, List

import pandas as pd

OFF_TO_MF_MAPPING = {
    "code": "mainUPC",
    "product_name": "foodDesc",
    "energy-kcal_100g": 208,
    "proteins_100g": 203,
    "fat_100g": 204,
    "carbohydrates_100g": 205,
    "fiber_100g": 291,
    "calcium_100g": 301,
    "iron_100g": 303,
    "magnesium_100g": 304,
    "potassium_100g": 306,
    "sodium_100g": 307,
    "zinc_100g": 309,
    "copper_100g": 312,
    "manganese_100g": 315,
    "selenium_100g": 317,
    "vitamin-c_100g": 401,
    "vitamin-b1_100g": 404,
    "vitamin-b2_100g": 405,
    "vitamin-pp_100g": 406,
    "vitamin-b6_100g": 415,
    "folates_100g": 417,
    "vitamin-b12_100g": 418,
    "vitamin-d_100g": 328,
    "vitamin-e_100g": 323,
    "vitamin-k_100g": 430,
    "choline_100g": 421,
    "saturated-fat_100g": 606,
    "monounsaturated-fat_100g": 645,
    "polyunsaturated-fat_100g": 646,
    "trans-fat_100g": 605,
    "cholesterol_100g": 601,
    "alcohol_100g": 221,
    "caffeine_100g": 262,
    "sugars_100g": 269,
    "omega-3-fat_100g": 901,
    "omega-6-fat_100g": 902,
    "vitamin-a_100g": 320,
}

CHUNK_SIZE = 75000

NAME_PREFIX = "off_mf_"

OFF_FOLDER_PATH = "data"

OFF_FILE_NAME = "en.openfoodfacts.org.products.csv"


def handle_brands(food: dict) -> None:
    if food["brands"] != 0:
        brands_list = food["brands"].split(",")
        brands_list = [x.strip() for x in brands_list]
        if len(brands_list) == 1:
            food["foodDesc"] = f'{food["foodDesc"]} by {brands_list[0]}'

    del food["brands"]


def add_default_weights(food: dict) -> None:
    food["weights"] = [
        {"gmWgt": "100.", "amount": 1, "sortOrder": 1, "msreDesc": "serving"},
        {"gmWgt": "28.35", "amount": 1, "sortOrder": 987, "msreDesc": "oz"},
        {"gmWgt": "1.", "amount": 1, "sortOrder": 988, "msreDesc": "gram"},
    ]

    food["dfSrv"] = {"gmWgt": "100.", "amount": 1, "msreDesc": "serving"}


def add_constants(food: dict) -> None:
    food["source"] = "OFF"
    food["common"] = False


def add_boost(food: dict) -> None:
    food["boost"] = 100 if food["brands"] != 0 else 25

    if food["ingredients_text"] != 0:
        food["boost"] += 25

    if food["serving_size"] != 0:
        food["boost"] += 25

    if food["serving_quantity"] != 0:
        food["boost"] += 25

    if food["image_small_url"] != 0:
        food["boost"] += 25

    del food["ingredients_text"]
    del food["serving_size"]
    del food["serving_quantity"]
    del food["image_small_url"]


def split_list_into_chunks(un_split_list, chunk_size) -> Generator[list, None, None]:
    for i in range(0, len(un_split_list), chunk_size):
        yield un_split_list[i : i + chunk_size]


def write_jsonl_chunks(chunk_list: List[List[dict]]) -> None:
    Path("output").mkdir(exist_ok=True)
    for index, chunk in enumerate(chunk_list):
        output_file_name = f"{NAME_PREFIX}{index}.jsonl"
        with open(os.path.join("output", output_file_name), "w") as f:
            for entry in chunk:
                json_out = json.dumps(entry) + "\n"
                f.write(json_out)


def main():
    off_df = pd.read_csv(
        os.path.join(OFF_FOLDER_PATH, OFF_FILE_NAME),
        sep="\t",
        quoting=csv.QUOTE_NONE,
        encoding="utf8",
        dtype={
            "code": str,
            "brands": str,
            "product_name": str,
            "serving_size": str,
            "countries_tags": str,
            "serving_quantity": float,
            "image_small_url": str,
            "ingredients_text": str,
            "energy-kcal_100g": float,
            "proteins_100g": float,
            "fat_100g": float,
            "carbohydrates_100g": float,
            "sugars_100g": float,
            "fiber_100g": float,
            "saturated-fat_100g": float,
            "monounsaturated-fat_100g": float,
            "polyunsaturated-fat_100g": float,
            "omega-3-fat_100g": float,
            "omega-6-fat_100g": float,
            "trans-fat_100g": float,
            "cholesterol_100g": float,
            "sodium_100g": float,
            "vitamin-a_100g": float,
            "vitamin-d_100g": float,
            "vitamin-e_100g": float,
            "vitamin-k_100g": float,
            "vitamin-c_100g": float,
            "vitamin-b1_100g": float,
            "vitamin-b2_100g": float,
            "vitamin-b6_100g": float,
            "vitamin-b9_100g": float,
            "folates_100g": float,
            "vitamin-b12_100g": float,
            "biotin_100g": float,
            "potassium_100g": float,
            "calcium_100g": float,
            "iron_100g": float,
            "magnesium_100g": float,
            "zinc_100g": float,
            "copper_100g": float,
            "manganese_100g": float,
            "selenium_100g": float,
            "chromium_100g": float,
            "molybdenum_100g": float,
            "iodine_100g": float,
            "caffeine_100g": float,
            "choline_100g": float,
            "alcohol_100g": float,
        },
        usecols=[
            "code",
            "brands",
            "product_name",
            "serving_size",
            "countries_tags",
            "serving_quantity",
            "image_small_url",
            "ingredients_text",
            "energy-kcal_100g",
            "proteins_100g",
            "fat_100g",
            "carbohydrates_100g",
            "sugars_100g",
            "fiber_100g",
            "saturated-fat_100g",
            "monounsaturated-fat_100g",
            "polyunsaturated-fat_100g",
            "omega-3-fat_100g",
            "omega-6-fat_100g",
            "trans-fat_100g",
            "cholesterol_100g",
            "sodium_100g",
            "vitamin-a_100g",
            "vitamin-d_100g",
            "vitamin-e_100g",
            "vitamin-k_100g",
            "vitamin-c_100g",
            "vitamin-b1_100g",
            "vitamin-b2_100g",
            "vitamin-b6_100g",
            "vitamin-b9_100g",
            "folates_100g",
            "vitamin-b12_100g",
            "biotin_100g",
            "potassium_100g",
            "calcium_100g",
            "iron_100g",
            "magnesium_100g",
            "zinc_100g",
            "copper_100g",
            "manganese_100g",
            "selenium_100g",
            "chromium_100g",
            "molybdenum_100g",
            "iodine_100g",
            "caffeine_100g",
            "choline_100g",
            "alcohol_100g",
        ],
    )
    off_df = off_df.dropna(
        axis=0,
        subset=[
            "code",
            "product_name",
            "energy-kcal_100g",
            "proteins_100g",
            "fat_100g",
            "carbohydrates_100g",
        ],
    )
    off_df = off_df.fillna(value=0)
    off_df = off_df.replace(r"\r+|\n+|\t+|\\N", "", regex=True)
    off_df = off_df.rename(columns=OFF_TO_MF_MAPPING)

    off_df = off_df[off_df["mainUPC"].str.len() > 7]
    off_df = off_df[off_df["mainUPC"].str.len() < 14]
    off_df = off_df[off_df["mainUPC"].str[:3] != "200"]

    off_df = off_df.drop(
        columns=[
            "countries_tags",
            "chromium_100g",
            "molybdenum_100g",
            "iodine_100g",
            "biotin_100g",
            "vitamin-b9_100g",
        ]
    )

    off_list = off_df.to_dict("records")

    for food in off_list:
        add_default_weights(food)
        add_constants(food)
        add_boost(food)

        handle_brands(food)

    off_chunk_list = list(split_list_into_chunks(off_list, CHUNK_SIZE))

    write_jsonl_chunks(off_chunk_list)


if __name__ == "__main__":
    main()
