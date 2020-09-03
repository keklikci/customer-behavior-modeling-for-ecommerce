#!/usr/bin/env python
# coding: utf-8

# create spark session & context
from pyspark.sql import SparkSession
from pyspark import SparkContext 

# session binning / sorting
from itertools import groupby
from operator import itemgetter

import time 
import math
from datetime import datetime
from json import dumps 
import statistics
# for machine count
import argparse

"""
*** << header indexes >> 
"""
DATE = 0
REFERRER_URL = 1
CURRENT_URL = 2
PAGE_TYPE = 3
PRODUCT_PRICE = 4
CART_AMOUNT = 5
USERID = 6
SESSIONID = 7
SEARCH_WORDS = 8
OLD_PRODUCT_PRICE = 9
PRODUCT_CATEGORY = 10
PAGE_CATEGORY = 11
PRODUCT_ID = 12

""" 
*** func_current_impl : raw_ids(sessions)
"""
def raw_ids(sessions):
    """Return the timestamp, sessionid, userid."""
    return [
        {"date": row[DATE], "userid": row[USERID], "sessionid": row[SESSIONID],}
        for session in sessions
        for row in session
    ]



""" 
*** func_current_impl : number_of_page_types(sessions)
"""
def number_of_page_types(sessions):
    """Returns number of pagetype up to row of session type.
    pagetypes can be category, main, other, productDetail, cart """
    results = []
    for session in sessions:

        current_counts = {
            "nb_of_cart": 0,
            "nb_of_productDetail": 0,
            "nb_of_other": 0,
            "nb_of_main": 0,
            "nb_of_category": 0,
            "nb_of_success": 0,
        }
        for row in session:
            if row[PAGE_TYPE] != "register":
                current_counts["nb_of_" + row[PAGE_TYPE]] += 1
            results.append(
                current_counts.copy()
            )  # copy is needed so appended values don't change later

    return results



""" 
*** func_current_impl : view_date(sessions)
"""
def view_date(sessions):
    results = []
    for session in sessions:
        last_time_cart_add = 0
        count_search_words = 0
        count_add = 0
        count_dec = 0
        last_cart_amount = 0.0
        set_of_cur_url = set()

        for i, row in enumerate(session):
            set_of_cur_url.add(row[CURRENT_URL])
            if row[SEARCH_WORDS]:
                count_search_words += 1

            current_cart_amount = row[CART_AMOUNT] if row[CART_AMOUNT] else 0
            if current_cart_amount > last_cart_amount:
                count_add += 1
            elif current_cart_amount < last_cart_amount:
                count_dec += 1
            count_products_cart = 0
            if row[PAGE_TYPE] == "cart":
                last_time_cart_add = row[DATE]
                count_products_cart = math.ceil(len(row[PRODUCT_ID]) / 36) if row[PRODUCT_ID] else 0
            results.append(
                {
                    "current_page_type": row[PAGE_TYPE],
                    "previous_page_type": session[i - 1][PAGE_TYPE] if i else None,
                    "first_page_type": session[0][PAGE_TYPE],
                    "session_length": (row[DATE] - session[0][DATE]).total_seconds(),
                    "number_of_pages": i + 1,
                    "cart_amount": row[CART_AMOUNT],
                    "weekday":
                        session[i][DATE].weekday(),
                    "hour_of_day": 
                        session[i][DATE].hour,
                    "time_elapsed_last_page": (row[DATE] - session[i-1][DATE]).total_seconds()
                    if i
                    else -1,
                    "time_elapsed_last_cart_add": 
                        (row[DATE] - last_time_cart_add).total_seconds()
                    if last_time_cart_add
                    else -1,
                    "number_of_products_in_cart": count_products_cart,
                    "number_of_search_words": count_search_words,
                    "nb_cart_add": count_add,
                    "nb_cart_dec": count_dec,
                    "number_of_unique_pages": len(set_of_cur_url),
                }
            )
            last_cart_amount = current_cart_amount
    return results



""" 
*** func_current_impl : viewed_item_features(sessions)
""" 
def viewed_item_features(sessions):
    """Return the features of viewed items (in productDetail page) """
    viewed_results = []
    for session in sessions:
        price_viewed_item = []
        price_old_item = []
        for row in session:
            if row[PAGE_TYPE] == "productDetail":
                price_viewed_item.append(row[PRODUCT_PRICE])
                price_old_item.append(row[OLD_PRODUCT_PRICE])
                viewed_results.append(
                    {
                        "current_price_viewed_item": price_viewed_item[-1],
                        "num_viewed_item": len(price_viewed_item),
                        "max_price_viewed_item": max(price_viewed_item),
                        "min_price_viewed_item": min(price_viewed_item),
                        "avg_price_viewed_item": sum(price_viewed_item)
                        / len(price_viewed_item),
                        "total_price_viewed_item": sum(price_viewed_item),
                        "discount_rate_viewed_item": (
                            price_old_item[-1] - price_viewed_item[-1]
                        )
                        / price_old_item[-1]
                        if price_viewed_item[-1]
                        else 0,
                    }
                )
            elif len(price_viewed_item) == 0:
                viewed_results.append(
                    {
                        "current_price_viewed_item": 0,
                        "num_viewed_item": 0,
                        "max_price_viewed_item": 0,
                        "min_price_viewed_item": 0,
                        "avg_price_viewed_item": 0,
                        "total_price_viewed_item": 0,
                        "discount_rate_viewed_item": 0,
                    }
                )
            else:
                viewed_results.append(
                    {
                        "current_price_viewed_item": 0,
                        "num_viewed_item": len(price_viewed_item),
                        "max_price_viewed_item": max(price_viewed_item),
                        "min_price_viewed_item": min(price_viewed_item),
                        "avg_price_viewed_item": sum(price_viewed_item)
                        / len(price_viewed_item),
                        "total_price_viewed_item": sum(price_viewed_item),
                        "discount_rate_viewed_item": 0,
                    }
                )
    return viewed_results



"""
*** func_current_impl : cart_item_features(sessions)
"""
def cart_item_features(sessions):
    """Return the features of cart items"""
    results = []
    items = {}  # will hold item details encountered in session
    dummy_item = (0, 0, 0)
    for session in sessions:
        (
            cart_amount,
            num_items,
            max_price,
            min_price,
            avg_price,
            avg_discount,
            discount_avg,
        ) = (0, 0, 0, 0, 0, 0, 0)
        for row in session:
            pids = row[PRODUCT_ID] if row[PRODUCT_ID] else []
            if row[PAGE_TYPE] == "productDetail":
                price = row[PRODUCT_PRICE]
                old_price = (
                    row[OLD_PRODUCT_PRICE] if row[OLD_PRODUCT_PRICE] else price
                )
                items[pids[0]] = (
                    price,
                    old_price,
                    1.0 - price / old_price if old_price else 0,
                )
            elif row[PAGE_TYPE] == "cart":
                cart_amount = row[CART_AMOUNT] if row[CART_AMOUNT] else 0
                num_items = math.ceil(len(row[PRODUCT_ID]) / 36) if row[PRODUCT_ID] else 0
                if num_items > 0:
                    item_prices = [items.get(pid, dummy_item)[0] for pid in pids]
                    item_old_prices = [items.get(pid, dummy_item)[1] for pid in pids]
                    discounts = [items.get(pid, dummy_item)[2] for pid in pids]
                    max_price = max(item_prices)
                    min_price = min(item_prices)
                    avg_price = cart_amount / num_items
                    avg_discount = (
                        1 - sum(item_prices) / sum(item_old_prices)
                        if sum(item_old_prices)
                        else 0
                    )
                    discount_avg = sum(discounts) / len(discounts) if discounts else 0
            results.append(
                {
                    "current_price_cart_item": cart_amount,
                    "num_cart_item": num_items,
                    "max_price_cart_item": max_price,
                    "min_price_cart_item": min_price,
                    "avg_price_cart_item": avg_price,
                    "avg_discount": avg_discount,
                    "discount_avg": discount_avg,
                }
            )
    return results



"""
*** func_current_impl : last_session_features(sessions)
"""
def _get_last_nb_page_types(session):
    """ Return the number of page types of last session """
    counts = {
        "last_nb_of_cart": 0,
        "last_nb_of_productDetail": 0,
        "last_nb_of_other": 0,
        "last_nb_of_main": 0,
        "last_nb_of_category": 0,
        "last_nb_of_success": 0,
    }
    if not session:
        return counts
    for row in session:
        if row[PAGE_TYPE] != "register":
            counts["last_nb_of_" + row[PAGE_TYPE]] += 1
    return counts

def _get_last_viewed_items(session):
    """ Return features for viewed items of last session"""
    price_viewed_item = []
    price_old_item = []
    for row in session:
        if row[PAGE_TYPE] == "productDetail":
            price_viewed_item.append(row[PRODUCT_PRICE])
            price_old_item.append(
                row[OLD_PRODUCT_PRICE]
                if row[OLD_PRODUCT_PRICE]
                else price_viewed_item[-1]
            )
    if len(price_viewed_item) == 0:
        viewed_results = {
            "last_num_viewed_item": 0,
            "last_max_price_viewed_item": 0,
            "last_min_price_viewed_item": 0,
            "last_avg_price_viewed_item": 0,
            "last_total_price_viewed_item": 0,
            "last_discount_rate_viewed_item": 0,
        }
    else:
        viewed_results = {
            "last_num_viewed_item": len(price_viewed_item),
            "last_max_price_viewed_item": max(price_viewed_item),
            "last_min_price_viewed_item": min(price_viewed_item),
            "last_avg_price_viewed_item": sum(price_viewed_item)
            / len(price_viewed_item),
            "last_total_price_viewed_item": sum(price_viewed_item),
            "last_discount_rate_viewed_item": (
                price_old_item[-1] - price_viewed_item[-1]
            )
            / price_old_item[-1]
            if price_viewed_item[-1]
            else 0,
        }
    return viewed_results

def _get_last_cart_item_features(session):
    (
        cart_amount,
        num_items,
        max_price,
        min_price,
        avg_price,
        avg_discount,
        discount_avg,
    ) = (0, 0, 0, 0, 0, 0, 0)
    items = {}
    dummy_item = (0, 0, 0)
    for row in session:
        pids = row[PRODUCT_ID] if row[PRODUCT_ID] else []
        if row[PAGE_TYPE] == "productDetail":
            price = row[PRODUCT_PRICE]
            old_price = (
                row[OLD_PRODUCT_PRICE] if row[OLD_PRODUCT_PRICE] else price
            )
            items[pids[0]] = (
                price,
                old_price,
                1.0 - price / old_price if old_price else 0,
            )
        elif row[PAGE_TYPE] == "cart":
            cart_amount = row[CART_AMOUNT] if row[CART_AMOUNT] else 0
            num_items = math.ceil(len(row[PRODUCT_ID]) / 36) if row[PRODUCT_ID] else 0
            if num_items > 0:
                item_prices = [items.get(pid, dummy_item)[0] for pid in pids]
                item_old_prices = [items.get(pid, dummy_item)[1] for pid in pids]
                discounts = [items.get(pid, dummy_item)[2] for pid in pids]
                max_price = max(item_prices)
                min_price = min(item_prices)
                avg_price = cart_amount / num_items
                avg_discount = (
                    1 - sum(item_prices) / sum(item_old_prices)
                    if sum(item_old_prices)
                    else 0
                )
                discount_avg = sum(discounts) / len(discounts) if discounts else 0
    return {
        "current_price_cart_item": cart_amount,
        "num_cart_item": num_items,
        "max_price_cart_item": max_price,
        "min_price_cart_item": min_price,
        "avg_price_cart_item": avg_price,
        "avg_discount": avg_discount,
        "discount_avg": discount_avg,
    }

def last_session_features(sessions):
    """Return the features of the last session of the user.
    Features of the previous session: 1st page, last page, session length, 
    number of page types, weekday and hour, viewed item features and cart item features."""
    last_session_results = []
    last_session = []
    for i, session in enumerate(sessions):
        session_start_time = session[0][DATE].timestamp()
        last_session_end_time = last_session[-1][DATE].timestamp() if last_session else 0
        results = {
            "last_first_page": last_session[0][PAGE_TYPE] if last_session else -1,
            "last_last_page": last_session[-1][PAGE_TYPE] if last_session else -1,
            "last_session_length": last_session[-1][DATE].timestamp()
            - last_session[0][DATE].timestamp()
            if last_session
            else -1,
            "last_time_since": session_start_time - last_session_end_time
            if last_session
            else -1,
            "last_num_viewed_pages": len(last_session) if last_session else 0,
            "last_weekday": 
                last_session[0][DATE].weekday()
            if last_session
            else -1,
            "last_hour_of_day": last_session[0][DATE].hour
            if last_session
            else -1,
        }
        results.update(_get_last_nb_page_types(last_session))
        results.update(_get_last_viewed_items(last_session))
        results.update(_get_last_cart_item_features(last_session))

        last_session_results.extend([results] * len(session))
        if (
            i < len(sessions) - 1
            and sessions[i + 1][0][USERID] == sessions[i][-1][USERID]
        ):
            last_session = session
        else:
            last_session = []

    return last_session_results



"""
*** func_current_impl : user_features(sessions)
"""
def user_features(sessions):
    results = []
    total_session_length = 0
    purchases = []
    row = []
    for i, session in enumerate(sessions):
        session_start_time = session[0][DATE].timestamp()
        count7 = 0
        count30 = 0
        count_all = 0
        sum7 = 0
        sum30 = 0
        for purchase in purchases:
            if session_start_time - purchase[0] < 7 * 24 * 3600:
                count7 += 1
                sum7 += purchase[1]
            if session_start_time - purchase[0] < 30 * 24 * 3600:
                count30 += 1
                sum30 += purchase[1]
            count_all += 1
        for row in session:
            if row[USERID] != session[-1][USERID] or not i:
                results.append(
                    {
                        "num_user_sessions": 0,
                        "mean_session_length": -1,
                        "mean_success_rate": -1,
                        "time_since_last_purchase": -1,
                        "purchases_in_last_7_days": 0,
                        "purchases_in_last_30_days": 0,
                        "money_spent_in_last_7_days": 0,
                        "money_spent_in_last_30_days": 0,
                    }
                )
            else:
                results.append(
                    {
                        "num_user_sessions": i,
                        "mean_session_length": total_session_length / i,
                        "mean_success_rate": count_all / i,
                        "time_since_last_purchase": row[DATE].timestamp() - purchases[-1][0]
                        if len(purchases)
                        else -1,
                        "purchases_in_last_7_days": count7,
                        "purchases_in_last_30_days": count30,
                        "money_spent_in_last_7_days": sum7,
                        "money_spent_in_last_30_days": sum30,
                    }
                )
            if row[PAGE_TYPE] == "success":
                purchases.append((row[DATE].timestamp(),row[CART_AMOUNT]))
        total_session_length += len(session)
    return results



"""
*** func_current_impl : product_id_features(sessions)
"""
def product_id_features(sessions):
    viewed_products = set()
    cart_products = set()
    results = []
    for session in sessions:
        for row in session:
            product_ids = row[PRODUCT_ID] if row[PRODUCT_ID] else []
            if row[PAGE_TYPE] == "cart":
                cart_products.update(product_ids)
            elif row[PAGE_TYPE] == "productDetail":
                viewed_products.update(product_ids)
            results.append(
                {
                    "viewed_products": dumps(list(viewed_products)),
                    "cart_products": dumps(list(cart_products)),
                }
            )
    return results



"""
*** func_current_impl : time_difference_features_purchases(sessions)
"""
def _get_time_difference_list(date_list):
    """Return time difference between consecutive elements of an ordered list and the first element
    is taken as the start of the data """
    if len(date_list) < 2:
        return [0]
    else:
        return [
            (t2 - t1).total_seconds()
            for t1, t2 in zip(date_list[:-1], date_list[1:])
        ]

def _get_mean_paper(elements):
    """ Return the mean of elements of a list according to the paper in which current elements have
    higher weight """
    a = list(range(1, len(elements) + 1))
    total_weight = 0
    for i in a:
        total_weight += i ** 2

    mean = 0

    for i, list_element in enumerate(elements):
        mean += (((i + 1) ** 2) / total_weight) * list_element

    return mean

def _get_std_paper(elements, mean):
    """ Return the standard deviation of elements of a list according to the paper in which current elements have
    higher weight """
    a = list(range(1, len(elements) + 1))
    total_weight = 0
    for i in a:
        total_weight += i ** 2
    variance = 0
    for i, list_element in enumerate(elements):
        variance += (((i + 1) ** 2) / total_weight) * (list_element - mean) ** 2

    return math.sqrt(variance)

def time_difference_features_purchases(sessions):
    """ Return features related to time difference between two consecutive purchases 
    Classes: -1 = the new user with no purchase
    1: normal user, 2: attrition, 3: at risk user, 4: lost user """
    results = []
    date_purchases = []
    values_purchases = []
    num_buy = 0
    num_detail = 0
    num_cart_add = 0
    for j, session in enumerate(sessions):
        for i, row in enumerate(session):
            current_cart_amount = row[CART_AMOUNT] if row[CART_AMOUNT] else 0
            if row[USERID] != sessions[j - 1][-1][USERID]:
                date_purchases = []
                values_purchases = []
                num_buy = 0
                num_detail = 0
                num_cart_add = 0

            if row[PAGE_TYPE] == "success":
                date_purchases.append(row[DATE])
                values_purchases.append(current_cart_amount)
                num_buy += 1
            elif row[PAGE_TYPE] == "productDetail":
                num_detail += 1
            elif row[PAGE_TYPE] == "cart":
                previuos_session_cart_amount =  session[i - 1][CART_AMOUNT] if session[i - 1][CART_AMOUNT] else 0 
                if current_cart_amount > previuos_session_cart_amount:
                    num_cart_add += 1
            if len(date_purchases) == 0:
                results.append(
                    {
                        "mean_time_between_purchases": -1,
                        "median_time_between_purchases": -1,
                        "std_time_between_purchases": -1,
                        "max_time_between_purchases": -1,
                        "current_time_diff_between_purchases": -1,
                        "mean_value_purchases": -1,
                        "median_value_purchases": -1,
                        "max_value_purchases": -1,
                        "last_purchase_value": -1,
                        "last2_purchase_value": -1,
                        "buy_to_detail_rate": num_buy / num_detail
                        if num_detail
                        else -1,
                        "cartadd_to_detail_rate": num_cart_add / num_detail
                        if num_detail
                        else -1,
                    }
                )
            else:
                time_difference = _get_time_difference_list(date_purchases)
                mean = _get_mean_paper(time_difference)
                std_dev = _get_std_paper(time_difference, mean)
                median = statistics.median(time_difference)
                """ Normal mean and standard deviation can be used as well as:"""
                """mean = statistics.mean(time_difference)
                std_dev = statistics.std(time_difference)"""

                current_time = row[DATE]
                current_time_dif = (current_time - date_purchases[-1]).total_seconds()
                mean_purchase = statistics.mean(values_purchases)
                median_purchase = statistics.median(values_purchases)

                results.append(
                    {
                        "mean_time_between_purchases": mean,
                        "median_time_between_purchases": median,
                        "std_time_between_purchases": std_dev,
                        "max_time_between_purchases": max(time_difference),
                        "current_time_diff_between_purchases": current_time_dif,
                        "mean_value_purchases": mean_purchase,
                        "median_value_purchases": median_purchase,
                        "max_value_purchases": max(values_purchases),
                        "last_purchase_value": values_purchases[-1],
                        "last2_purchase_value": values_purchases[-2]
                        if len(values_purchases) > 1
                        else -1,
                        "buy_to_detail_rate": num_buy / num_detail
                        if num_detail
                        else -1,
                        "cartadd_to_detail_rate": num_cart_add / num_detail
                        if num_detail
                        else -1,
                    }
                )
    return results



"""
*** func_current_impl : session_features_before_buy(sessions)
"""
def session_features_before_buy(sessions):
    """Return features of sessions between consecutive purchases and after a purchase """
    results = []
    session_durations_bf_buy = []
    num_sessions_bf_buy = []
    num_page_view_bf_buy = []
    num_sessions = 0
    num_page_view = 0
    num_sessions_after_buy = 0
    num_pageview_after_buy = 0
    for j, session in enumerate(sessions):
        num_sessions += 1
        num_sessions_after_buy += 1
        for i, row in enumerate(session):
            num_page_view += 1
            num_pageview_after_buy += 1
            if row[USERID] != sessions[j - 1][-1][USERID]:
                session_durations_bf_buy = []
                num_sessions_bf_buy = []
                num_page_view_bf_buy = []
            if row[PAGE_TYPE] == "success":
                session_durations_bf_buy.append(
                    (row[DATE] - sessions[j][0][DATE]).total_seconds()
                )
                num_sessions_bf_buy.append(num_sessions)
                num_page_view_bf_buy.append(num_page_view)
                num_sessions = 0
                num_page_view = 0

            if len(session_durations_bf_buy) == 0:
                results.append(
                    {
                        "mean_duration_bf_buy": -1,
                        "median_duration_bf_buy": -1,
                        "mex_duration_bf_buy": -1,
                        "last_duration_bf_buy": -1,
                        "median_num_sessions_bf_buy": -1,
                        "last_num_sessions_bf_buy": -1,
                        "median_num_pageview_bf_buy": -1,
                        "last_num_pageview_bf_buy": -1,
                        "num_sessions_after_buy": num_sessions_after_buy,
                        "num_pageview_after_buy": num_pageview_after_buy,
                    }
                )
            else:
                results.append(
                    {
                        "mean_duration_bf_buy": statistics.mean(
                            session_durations_bf_buy
                        ),
                        "median_duration_bf_buy": statistics.median(
                            session_durations_bf_buy
                        ),
                        "mex_duration_bf_buy": max(session_durations_bf_buy),
                        "last_duration_bf_buy": session_durations_bf_buy[-1],
                        "median_num_sessions_bf_buy": statistics.median(
                            num_sessions_bf_buy
                        ),
                        "last_num_sessions_bf_buy": num_sessions_bf_buy[-1],
                        "median_num_pageview_bf_buy": statistics.median(
                            num_page_view_bf_buy
                        ),
                        "last_num_pageview_bf_buy": num_page_view_bf_buy[-1],
                        "num_sessions_after_buy": num_sessions_after_buy,
                        "num_pageview_after_buy": num_pageview_after_buy,
                    }
                )
            if row[PAGE_TYPE] == "success":
                num_pageview_after_buy = 0
                num_sessions_after_buy = 0
    return results



"""
*** << ALL FEATURES >> 
"""
ALL_FEATURES = [
    raw_ids,
    view_date,
    number_of_page_types,
    user_features,
    viewed_item_features,
    cart_item_features,
    last_session_features,
    product_id_features,
    session_features_before_buy,
    time_difference_features_purchases,
]



"""
*** << ALL LABELS >> 
"""
def label_success_current_session(sessions):
    results = []
    for session in sessions:
        success = 0
        for row in session:
            if row[PAGE_TYPE] == "success":
                success = 1
                break
        for row in session:
            results.append({"label_success_current_session": success})
    return results


def label_success_X_day(sessions):
    """Success in 7, 14 or 30 days."""
    results = []
    next_success = 10 ** 100  # large number
    for session in sessions[::-1]:
        current_session_success = None
        for row in session[::-1]:
            current_time = row[DATE].timestamp()
            if row[PAGE_TYPE] == "success":
                current_session_success = current_time
            if next_success - current_time < 7 * 24 * 3600:
                results.append(
                    {
                        "label_success_7_day": 1,
                        "label_success_14_day": 1,
                        "label_success_30_day": 1,
                    }
                )
            elif next_success - current_time < 14 * 24 * 3600:
                results.append(
                    {
                        "label_success_7_day": 0,
                        "label_success_14_day": 1,
                        "label_success_30_day": 1,
                    }
                )
            elif next_success - current_time < 30 * 24 * 3600:
                results.append(
                    {
                        "label_success_7_day": 0,
                        "label_success_14_day": 0,
                        "label_success_30_day": 1,
                    }
                )
            else:
                results.append(
                    {
                        "label_success_7_day": 0,
                        "label_success_14_day": 0,
                        "label_success_30_day": 0,
                    }
                )
        if current_session_success:
            next_success = current_session_success
    results.reverse()
    return results

ALL_LABELS = [label_success_current_session, label_success_X_day]


def user_sessions(sessions):
    sessions = sessions.combineByKey(lambda row: row, 
                              lambda rows, row: rows + row, 
                              lambda rows1, rows2: rows1 + rows2
                              )
    sessions = sessions.map(lambda row: [row[1][i] for i in range(len(row[1]))])

    sessions = sessions.map(lambda rows:  [sorted([session for session in rows], key=itemgetter(0))
                                    for (key, rows) in 
                                    groupby(sorted(rows,key=itemgetter(7)), itemgetter(7))])
    return sessions


"""
*** << MAIN CHECKPOINT >> 
"""
def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('numberOfMachines', type= int)
    parser.add_argument('inputFile')
    args = parser.parse_args()
    numberOfMachines = vars(args).get('numberOfMachines')
    fname = vars(args).get('inputFile')

    spark = SparkSession \
    .builder \
    .appName("pusulaInsider") \
    .getOrCreate()

    """ timing start """
    start_time = time.time()

    """ read first 10K rows to spark dataframe """
    sdf = spark.read.parquet(fname)

    """ 
    *** cast string date to timestamp 
    """
    from pyspark.sql.types import TimestampType
    sdf = sdf.withColumn("date",sdf["date"].cast(TimestampType()))

    sessions = sdf.rdd.map(lambda row: [row]
                               ).keyBy(lambda row: row[DATE][USERID])
    """bin user sessions"""
    sessions = user_sessions(sessions)
    sessions.cache()
    sessions.first()

    data = sessions.map(lambda sessions: [feature_func(sessions) for feature_func in (ALL_FEATURES + ALL_LABELS)])
    data = data.flatMap(lambda session: zip(*session))
    data.first()

    """ timing end """
    end_time = time.time()
    print("Execution time: {0:.2f} sec".format(end_time - start_time))

    """ save output """
    data.saveAsTextFile("/home/matalay/Workspace/pusulaInsiderOutput")

    """ destroy spark session & context
     uncomment the following lines if still desire to display SPARK_UI after execution """
    spark.stop()

if __name__ == "__main__":
    main()

