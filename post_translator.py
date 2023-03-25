import ast
import logging
import os.path
import time

import pandas as pd
from googletrans import Translator

import logger


def stop_and_save(log: logging.Logger, trans_new: list, df_trans: pd.DataFrame, dest_path: str) -> pd.DataFrame:
    """
    Сохранение переведённых публикаций с дозаписью в существующий dataframe.

    :param dest_path:   путь для сохранения
    :param log:         класс логгера
    :param trans_new:   список переведённых постов
    :param df_trans:    прочитанный dataframe переведённых публикаций
    :return:            обновлённый dataframe переведённых публикаций
    """

    df_trans_new = pd.DataFrame(trans_new, columns=["id", "text", "num"])
    log.debug(f"Дозаписываем {df_trans_new.shape[0]} публикаций к имеющимся {df_trans.shape[0]} публикациям")
    df_trans = pd.concat([df_trans, df_trans_new])
    log.debug(f"Новое количество переведённых публикаций: {df_trans.shape[0]}")
    df_trans.to_csv(dest_path, index=False)
    return df_trans


def main():
    """
    Перевод публикаций
    :return:
    """
    log = logger.logger_init("posts_translate")
    log.info("Start publications translate")

    src_path = "csv/posts/elagin_posts_with_adv.csv"
    dest_path = "csv/posts/posts_en2.csv"

    if not os.path.exists(src_path):
        log.error(f"Файл по исходному пути {src_path} не найден")
        return 0

    df_full = pd.read_csv(src_path)
    df_clean_ad_posts = df_full[df_full["isad"] == 0][["id", "content", "content_lemm"]]
    df_clean_ad_posts["content_lemm"] = df_clean_ad_posts["content_lemm"].apply(lambda x: ast.literal_eval(x))
    df_clean_ad_posts["word_count"] = df_clean_ad_posts["content_lemm"].apply(lambda x: len(x))
    df = df_clean_ad_posts[df_clean_ad_posts["word_count"] != 0]
    log.debug(f"Dataframe с русскоязычными постами содержит {df.shape[0]} строк")

    if os.path.exists(dest_path):
        df_translated = pd.read_csv(dest_path)
        log.debug(f"Прочитан файл с {df_translated.shape[0]} переведёнными публикациями")
    else:
        df_translated = pd.DataFrame()
        log.debug("Переведенных публикаций не найдено")

    i = 0
    trans_new = []
    if df_translated.shape[0] > 0:
        start_i = df_translated["num"].max() + 1
    else:
        start_i = i
    log.debug(f"Start publication num: {start_i}")
    translator = Translator(service_urls=['translate.google.com']) 
    for post in df[["id", "content"]].values:
        if i >= start_i:
            if i % 100 == 0 and i != 0:
                log.debug(i)
            try:
                trans_new.append([post[0], translator.translate(post[1], dest='en').text, i])
            except KeyboardInterrupt as e:
                # сохранение переведённых записей в случае ручной остановки программы
                log.warning("Сохранение записей в результате остановки скрипта")
                stop_and_save(log, trans_new, df_translated, dest_path)
                return 0
            except Exception as e:
                log.warning("Ошибка перевода или таймаут от сервера записей в результате остановки скрипта")
                log.warning(f"id: {post[0]}, номер поста: {i}, traceback: {e}")
                time.sleep(3)

            if i % 500 == 0 and i != 0:
                # сохранение каждых 500 переведенных постов
                df_translated = stop_and_save(log, trans_new, df_translated, dest_path)
                trans_new = []
        i += 1
    stop_and_save(log, trans_new, df_translated, dest_path)
    return 0


if __name__ == "__main__":
    main()
