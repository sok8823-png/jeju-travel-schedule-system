from __future__ import annotations

import math
import random
from typing import List, Dict, Any, Optional
from datetime import date, timedelta   # ← 이 줄 추가

from db import get_connection


# --------------------------------------------------------------------
# 공통 유틸
# --------------------------------------------------------------------

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """위경도로 거리(km) 계산"""
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lam = math.radians(lon2 - lon1)

    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lam / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


# --------------------------------------------------------------------
# 1. 관광지 + 맛집 기본 추천 (그대로 사용)
# --------------------------------------------------------------------

def recommend_spot_with_restaurant(limit: int = 5) -> List[Dict[str, Any]]:
    """
    평점 높은 관광지 + 주변 식당 1곳씩 묶어서 추천.
    (날씨/개인화 고려 X : 가장 단순한 버전)
    """
    conn = get_connection()
    result: List[Dict[str, Any]] = []

    try:
        with conn.cursor() as cur:
            # 평점 높은 관광지 상위 limit개
            sql_spots = """
                SELECT id, name, rating
                FROM jeju_tour_spots_info
                ORDER BY rating DESC, review_count DESC
                LIMIT %s;
            """
            cur.execute(sql_spots, (limit,))
            spots = cur.fetchall()

            for spot in spots:
                spot_id = spot["id"]

                sql_food = """
                    SELECT
                        r.id         AS restaurant_id,
                        r.store_name AS restaurant_name,
                        r.rating     AS restaurant_rating,
                        m.distance_km
                    FROM spot_restaurant_map AS m
                    JOIN seogwipo_restaurants AS r
                        ON r.id = m.restaurant_id
                    WHERE m.spot_id = %s
                    ORDER BY r.rating DESC, m.distance_km ASC
                    LIMIT 1;
                """
                cur.execute(sql_food, (spot_id,))
                food = cur.fetchone()

                if food is None:
                    item = {
                        "spot_id": spot["id"],
                        "spot_name": spot["name"],
                        "spot_rating": float(spot["rating"]),
                        "restaurant_id": None,
                        "restaurant_name": None,
                        "restaurant_rating": None,
                        "distance_km": None,
                    }
                else:
                    item = {
                        "spot_id": spot["id"],
                        "spot_name": spot["name"],
                        "spot_rating": float(spot["rating"]),
                        "restaurant_id": food["restaurant_id"],
                        "restaurant_name": food["restaurant_name"],
                        "restaurant_rating": float(food["restaurant_rating"]),
                        "distance_km": float(food["distance_km"]),
                    }

                result.append(item)

    finally:
        conn.close()

    return result


# --------------------------------------------------------------------
# 2. 여행자 Preferred_Food 기반 식당 추천 (그대로 사용)
# --------------------------------------------------------------------

def _get_preferred_food(traveler_id: int) -> Optional[str]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT Preferred_Food
                FROM TRAVELER_PROFILE
                WHERE Traveler_ID = %s
            """
            cur.execute(sql, (traveler_id,))
            row = cur.fetchone()
            if row and row["Preferred_Food"]:
                return row["Preferred_Food"]
            return None
    finally:
        conn.close()


def recommend_for_traveler(traveler_id: int, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Traveler_Profile.Preferred_Food 기반으로
    선호 음식(한식/일식/해산물.. 등)에 맞는 식당을 우선 추천.
    """
    preferred_food = _get_preferred_food(traveler_id)

    # 키워드 매핑 (DB 실제 값에 맞춰 필요시 수정)
    keyword_map = {
        "한식 위주 음식": ["한식"],
        "일식 위주 음식": ["일식"],
        "중식 위주 음식": ["중식"],
        "해산물 위주 음식": ["해산물", "횟집", "생선"],
        "카페 위주": ["카페", "커피"],
    }
    keywords = keyword_map.get(preferred_food, [])

    conn = get_connection()
    result: List[Dict[str, Any]] = []

    try:
        with conn.cursor() as cur:
            # 관광지 상위 limit개
            sql_spots = """
                SELECT id, name, rating
                FROM jeju_tour_spots_info
                ORDER BY rating DESC, review_count DESC
                LIMIT %s;
            """
            cur.execute(sql_spots, (limit,))
            spots = cur.fetchall()

            for spot in spots:
                spot_id = spot["id"]

                base_sql = """
                    SELECT
                        r.id         AS restaurant_id,
                        r.store_name AS restaurant_name,
                        r.biz_type_detail,
                        r.rating     AS restaurant_rating,
                        m.distance_km
                    FROM spot_restaurant_map AS m
                    JOIN seogwipo_restaurants AS r
                        ON r.id = m.restaurant_id
                    WHERE m.spot_id = %s
                """
                params: List[Any] = [spot_id]

                if keywords:
                    like_clauses = []
                    for kw in keywords:
                        like_clauses.append("r.biz_type_detail LIKE %s")
                        params.append(f"%{kw}%")
                    base_sql += " AND (" + " OR ".join(like_clauses) + ")"

                base_sql += """
                    ORDER BY r.rating DESC, m.distance_km ASC
                    LIMIT 1;
                """

                cur.execute(base_sql, params)
                food = cur.fetchone()

                # 선호 필터로도 안 나오면, 필터 없이 한 번 더
                if food is None:
                    fallback_sql = """
                        SELECT
                            r.id         AS restaurant_id,
                            r.store_name AS restaurant_name,
                            r.rating     AS restaurant_rating,
                            m.distance_km
                        FROM spot_restaurant_map AS m
                        JOIN seogwipo_restaurants AS r
                            ON r.id = m.restaurant_id
                        WHERE m.spot_id = %s
                        ORDER BY r.rating DESC, m.distance_km ASC
                        LIMIT 1;
                    """
                    cur.execute(fallback_sql, (spot_id,))
                    food = cur.fetchone()


                if food is None:
                    item = {
                        "spot_id": spot["id"],
                        "spot_name": spot["name"],
                        "spot_rating": float(spot["rating"]),
                        "restaurant_id": None,
                        "restaurant_name": None,
                        "restaurant_rating": None,
                        "distance_km": None,
                    }
                else:
                    item = {
                        "spot_id": spot["id"],
                        "spot_name": spot["name"],
                        "spot_rating": float(spot["rating"]),
                        "restaurant_id": food["restaurant_id"],
                        "restaurant_name": food["restaurant_name"],
                        "restaurant_rating": float(food["restaurant_rating"]),
                        "distance_km": float(food["distance_km"]),
                    }

                result.append(item)
    finally:
        conn.close()

    return result


# --------------------------------------------------------------------
# 3. 일정 생성 관련 공통 함수
# --------------------------------------------------------------------

def _get_traveler_profile(traveler_id: int) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            sql = "SELECT * FROM TRAVELER_PROFILE WHERE Traveler_ID = %s"
            cur.execute(sql, (traveler_id,))
            return cur.fetchone()
    finally:
        conn.close()


def _get_spot_by_id(spot_id: int) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, category, rating, inout_door, lat, lon, review_count
                FROM jeju_tour_spots_info
                WHERE id = %s
                """,
                (spot_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def _get_restaurant_by_id(rest_id: int) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, store_name, rating, lat, lon FROM seogwipo_restaurants WHERE id = %s",
                (rest_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def _get_all_spots_for_weather(weather_mode: str) -> List[Dict[str, Any]]:
    """
    날씨 모드에 맞는 전체 후보 관광지 리스트 (평점 MIN_RATING~5.0) 가져오기.
    """
    if weather_mode == "rainy":
        inout_filter = ("실내", "복합")
    else:
        inout_filter = ("실내", "실외", "복합")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            inout_clause = ",".join(["%s"] * len(inout_filter))
            sql = f"""
                SELECT id, name, category, rating, inout_door, lat, lon, review_count
                FROM jeju_tour_spots_info
                WHERE rating BETWEEN %s AND 5.0
                  AND inout_door IN ({inout_clause})
                ORDER BY rating DESC, review_count DESC
            """
            params: List[Any] = [MIN_RATING, *inout_filter]
            cur.execute(sql, params)
            rows = cur.fetchall()
            return list(rows)
    finally:
        conn.close()



def _string_contains(source: Optional[str], keyword: Optional[str]) -> bool:
    if not source or not keyword:
        return False
    return keyword in source


MIN_RATING = 3.5  # 👈 원하면 3.0, 4.0 등으로 바꿔서 사용

def _get_neighbor_spots(
    base_spot_id: int,
    exclude_ids: List[int],
    weather_mode: str,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """
    spot_spot_map을 사용해 base_spot_id 인근의 spot 후보를 찾는다.
    날씨 모드에 맞는 inout_door + 평점 조건을 함께 적용.
    """
    if weather_mode == "rainy":
        inout_filter = ("실내", "복합")
    else:
        inout_filter = ("실내", "실외", "복합")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # IN 절 자리 만들어 주기
            inout_clause = ",".join(["%s"] * len(inout_filter))

            sql = f"""
                SELECT
                    s.id,
                    s.name,
                    s.category,
                    s.rating,
                    s.inout_door,
                    s.lat,
                    s.lon,
                    s.review_count,
                    m.distance_km
                FROM spot_spot_map AS m
                JOIN jeju_tour_spots_info AS s
                    ON s.id = m.spot_id_2
                WHERE m.spot_id_1 = %s
                  AND s.rating BETWEEN %s AND 5.0
                  AND s.inout_door IN ({inout_clause})
            """

            params: List[Any] = [base_spot_id, MIN_RATING, *inout_filter]

            if exclude_ids:
                # 이미 사용한 spot은 제외
                excl_clause = ",".join(["%s"] * len(exclude_ids))
                sql += f" AND s.id NOT IN ({excl_clause})"
                params.extend(exclude_ids)

            sql += """
                ORDER BY s.rating DESC, s.review_count DESC, m.distance_km ASC
                LIMIT %s
            """
            params.append(limit)

            cur.execute(sql, params)
            rows = cur.fetchall()
            return list(rows)
    finally:
        conn.close()



def _get_restaurant_for_spot(
    spot_id: int,
    preferred_food: Optional[str],
    exclude_ids: List[int],
) -> Optional[Dict[str, Any]]:
    """
    특정 관광지 주변 '밥집(일반음식점)' 1개 선택.
    preferred_food 기반 필터 후, 없으면 일반음식점 전체에서 선택.
    exclude_ids: 이미 사용한 식당 id 리스트
    """
    keyword_map = {
        "한식 위주 음식": ["한식"],
        "일식 위주 음식": ["일식"],
        "중식 위주 음식": ["중식"],
        "해산물 위주 음식": ["해산물", "횟집", "생선"],
        # 카페는 여기서 쓰지 않음 (밥집 전용)
    }
    keywords = keyword_map.get(preferred_food, [])

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            base_sql = """
                SELECT
                    r.id         AS restaurant_id,
                    r.store_name AS restaurant_name,
                    r.biz_type,
                    r.biz_type_detail,
                    r.rating     AS restaurant_rating,
                    r.lat, r.lon,
                    m.distance_km
                FROM spot_restaurant_map AS m
                JOIN seogwipo_restaurants AS r
                    ON r.id = m.restaurant_id
                WHERE m.spot_id = %s
                  AND r.biz_type = '일반음식점'
            """
            params: List[Any] = [spot_id]

            if exclude_ids:
                in_clause = ",".join(["%s"] * len(exclude_ids))
                base_sql += f" AND r.id NOT IN ({in_clause})"
                params.extend(exclude_ids)

            if keywords:
                like_clauses = []
                for kw in keywords:
                    like_clauses.append("r.biz_type_detail LIKE %s")
                    params.append(f"%{kw}%")
                base_sql += " AND (" + " OR ".join(like_clauses) + ")"

            base_sql += """
                ORDER BY r.rating DESC, m.distance_km ASC
                LIMIT 1;
            """

            cur.execute(base_sql, params)
            row = cur.fetchone()

            # 선호 필터로도 안 나오면, 일반음식점 전체에서 평점 우선 선택
            if row is None:
                params2: List[Any] = [spot_id]
                sql2 = """
                    SELECT
                        r.id         AS restaurant_id,
                        r.store_name AS restaurant_name,
                        r.rating     AS restaurant_rating,
                        r.lat, r.lon,
                        m.distance_km
                    FROM spot_restaurant_map AS m
                    JOIN seogwipo_restaurants AS r
                        ON r.id = m.restaurant_id
                    WHERE m.spot_id = %s
                      AND r.biz_type = '일반음식점'
                """
                if exclude_ids:
                    in_clause = ",".join(["%s"] * len(exclude_ids))
                    sql2 += f" AND r.id NOT IN ({in_clause})"
                    params2.extend(exclude_ids)

                sql2 += """
                    ORDER BY r.rating DESC, m.distance_km ASC
                    LIMIT 1;
                """
                cur.execute(sql2, params2)
                row = cur.fetchone()

            return row
    finally:
        conn.close()

def _get_cafe_for_spot(
    spot_id: int,
    exclude_ids: List[int],
) -> Optional[Dict[str, Any]]:
    """
    특정 관광지 주변 '카페(휴게음식점)' 1개 선택.
    카페/커피 키워드를 우선 적용, 없으면 휴게음식점 전체에서 선택.
    """
    cafe_keywords = ["카페", "커피"]

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            base_sql = """
                SELECT
                    r.id         AS restaurant_id,
                    r.store_name AS restaurant_name,
                    r.biz_type,
                    r.biz_type_detail,
                    r.rating     AS restaurant_rating,
                    r.lat, r.lon,
                    m.distance_km
                FROM spot_restaurant_map AS m
                JOIN seogwipo_restaurants AS r
                    ON r.id = m.restaurant_id
                WHERE m.spot_id = %s
                  AND r.biz_type = '휴게음식점'
            """
            params: List[Any] = [spot_id]

            if exclude_ids:
                in_clause = ",".join(["%s"] * len(exclude_ids))
                base_sql += f" AND r.id NOT IN ({in_clause})"
                params.extend(exclude_ids)

            # 카페/커피 키워드 우선
            like_clauses = []
            for kw in cafe_keywords:
                like_clauses.append("r.biz_type_detail LIKE %s")
                params.append(f"%{kw}%")
            base_sql += " AND (" + " OR ".join(like_clauses) + ")"

            base_sql += """
                ORDER BY r.rating DESC, m.distance_km ASC
                LIMIT 1;
            """

            cur.execute(base_sql, params)
            row = cur.fetchone()

            # 키워드로도 안 나오면, 휴게음식점 전체에서 평점 우선 선택
            if row is None:
                params2: List[Any] = [spot_id]
                sql2 = """
                    SELECT
                        r.id         AS restaurant_id,
                        r.store_name AS restaurant_name,
                        r.rating     AS restaurant_rating,
                        r.lat, r.lon,
                        m.distance_km
                    FROM spot_restaurant_map AS m
                    JOIN seogwipo_restaurants AS r
                        ON r.id = m.restaurant_id
                    WHERE m.spot_id = %s
                      AND r.biz_type = '휴게음식점'
                """
                if exclude_ids:
                    in_clause = ",".join(["%s"] * len(exclude_ids))
                    sql2 += f" AND r.id NOT IN ({in_clause})"
                    params2.extend(exclude_ids)

                sql2 += """
                    ORDER BY r.rating DESC, m.distance_km ASC
                    LIMIT 1;
                """
                cur.execute(sql2, params2)
                row = cur.fetchone()

            return row
    finally:
        conn.close()


def _calc_distance_spot_to_restaurant(spot_id: int, restaurant_id: int) -> Optional[float]:
    """
    spot_restaurant_map에 있으면 그 값을 사용, 없으면 위경도로 계산.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT distance_km
                FROM spot_restaurant_map
                WHERE spot_id = %s AND restaurant_id = %s
            """
            cur.execute(sql, (spot_id, restaurant_id))
            row = cur.fetchone()
            if row and row["distance_km"] is not None:
                return float(row["distance_km"])

            # 매핑이 없으면 위경도로 계산
            spot = _get_spot_by_id(spot_id)
            if not spot:
                return None

            cur.execute(
                "SELECT lat, lon FROM seogwipo_restaurants WHERE id = %s",
                (restaurant_id,),
            )
            rest = cur.fetchone()
            if not rest:
                return None

            return haversine(
                float(spot["lat"]),
                float(spot["lon"]),
                float(rest["lat"]),
                float(rest["lon"]),
            )
    finally:
        conn.close()


def _calc_distance_restaurant_to_spot(restaurant_id: int, spot_id: int) -> Optional[float]:
    """
    식당 → 관광지 거리 계산 (역방향 매핑 없으면 위경도).
    """
    # 거리 대칭 가정
    return _calc_distance_spot_to_restaurant(spot_id=spot_id, restaurant_id=restaurant_id)


def _calc_distance_spot_to_spot(spot_id_1: int, spot_id_2: int) -> Optional[float]:
    """
    관광지 ↔ 관광지 거리 계산 (spot_spot_map 있으면 사용, 없으면 위경도).
    """
    if spot_id_1 == spot_id_2:
        return 0.0

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT distance_km
                FROM spot_spot_map
                WHERE (spot_id_1 = %s AND spot_id_2 = %s)
                   OR (spot_id_1 = %s AND spot_id_2 = %s)
            """
            cur.execute(sql, (spot_id_1, spot_id_2, spot_id_2, spot_id_1))
            row = cur.fetchone()
            if row and row["distance_km"] is not None:
                return float(row["distance_km"])

            # 없으면 위경도 계산
            s1 = _get_spot_by_id(spot_id_1)
            s2 = _get_spot_by_id(spot_id_2)
            if not s1 or not s2:
                return None

            return haversine(
                float(s1["lat"]),
                float(s1["lon"]),
                float(s2["lat"]),
                float(s2["lon"]),
            )
    finally:
        conn.close()


def _calc_distance_restaurant_to_restaurant(rest_id_1: int, rest_id_2: int) -> Optional[float]:
    """
    식당 ↔ 식당 거리 (위경도로 계산).
    """
    if rest_id_1 == rest_id_2:
        return 0.0

    r1 = _get_restaurant_by_id(rest_id_1)
    r2 = _get_restaurant_by_id(rest_id_2)
    if not r1 or not r2:
        return None

    return haversine(
        float(r1["lat"]),
        float(r1["lon"]),
        float(r2["lat"]),
        float(r2["lon"]),
    )

def _calc_leg_distance(prev_item: Dict[str, Any], cur_item: Dict[str, Any]) -> Optional[float]:
    """
    스케줄 상에서 바로 이전 장소(prev_item) → 현재 장소(cur_item)까지의 이동 거리 계산.
    spot / restaurant / cafe 를 모두 처리한다.
    """
    if prev_item is None:
        return None

    prev_type = prev_item["type"]
    cur_type = cur_item["type"]

    def is_restaurant(t: str) -> bool:
        # 밥집(restaurant)과 카페(cafe)를 모두 "식당류"로 취급
        return t in ("restaurant", "cafe")

    # spot → 식당류(밥집/카페)
    if prev_type == "spot" and is_restaurant(cur_type):
        return _calc_distance_spot_to_restaurant(prev_item["spot_id"], cur_item["restaurant_id"])

    # 식당류 → spot
    if is_restaurant(prev_type) and cur_type == "spot":
        return _calc_distance_restaurant_to_spot(prev_item["restaurant_id"], cur_item["spot_id"])

    # spot ↔ spot
    if prev_type == "spot" and cur_type == "spot":
        return _calc_distance_spot_to_spot(prev_item["spot_id"], cur_item["spot_id"])

    # 식당류 ↔ 식당류 (밥집↔밥집, 밥집↔카페, 카페↔카페 모두 포함)
    if is_restaurant(prev_type) and is_restaurant(cur_type):
        return _calc_distance_restaurant_to_restaurant(prev_item["restaurant_id"], cur_item["restaurant_id"])

    return None



def _fill_distances_for_day(day_items: List[Dict[str, Any]]) -> None:
    """
    같은 day에 속한 스케줄 항목들에 대해
    order 순서대로 이전 장소 → 현재 장소 거리(distance_km)를 채운다.
    """
    day_items.sort(key=lambda x: x["order"])
    prev: Optional[Dict[str, Any]] = None
    for item in day_items:
        if prev is None:
            item["distance_km"] = None
        else:
            item["distance_km"] = _calc_leg_distance(prev, item)
        prev = item


def _choose_next_spot(
    *,
    all_spots: List[Dict[str, Any]],
    used_spot_ids: set[int],
    style_pref: Optional[str],
    weather_mode: str,
    base_spot_id: Optional[int],
) -> Optional[Dict[str, Any]]:
    """
    다음 방문할 관광지 선택 로직.

    - base_spot_id가 있을 때: 그날 이미 방문한 spot가 있음 → 그 주변 먼저 탐색
    - base_spot_id가 없을 때: 그날의 첫 spot → 전역(all_spots)에서 스타일 우선 랜덤

    비가 오든 안 오든, 여기서는 실내/실외/복합을 따로 가중치 주지 않고
    "스타일 매칭 → 아무 곳" 우선순위만 사용한다.
    (비 오는 날에는 all_spots 자체가 이미 실외가 제외된 상태라고 가정)
    """

    # 1) 스타일 매칭 함수 (음식점 keyword_map 과 비슷한 구조)
    def match_style(s: Dict[str, Any]) -> bool:
        """
        traveler의 style_pref 문자열을 보고 스타일 그룹을 결정한 뒤,
        category 안에서만 키워드로 매칭한다.
        예: style_pref 에 '문화'가 들어 있으면 그룹 '문화' 사용.
        """

        if not style_pref:
            return False

        # style_pref 문자열 안에서 어떤 그룹인지 판별
        style_group: Optional[str] = None
        if "문화" in style_pref:
            style_group = "문화"
        elif "자연" in style_pref:
            style_group = "자연"
        elif "액티비티" in style_pref or "체험" in style_pref:
            style_group = "액티비티"
        elif "휴양" in style_pref or "휴식" in style_pref:
            style_group = "휴양"

        if not style_group:
            return False

        # 이 함수 안에서만 쓰는 style용 keyword_map
        style_keyword_map: Dict[str, List[str]] = {
            # category 에 들어 있을 법한 단어 기준으로 세팅
            "문화": ["문화", "역사", "박물관", "전시", "유적", "전통", "예술"],
            "자연": ["자연", "산", "계곡", "바다", "해변", "오름", "숲", "공원", "폭포"],
            "액티비티": ["체험", "레저", "액티비티", "카트", "서핑", "승마", "스포츠"],
            "휴양": ["휴양", "스파", "온천", "리조트", "펜션"],
        }

        keywords = style_keyword_map.get(style_group, [])
        if not keywords:
            return False

        category_text = (s.get("category") or "").lower()

        # category 에 키워드가 하나라도 포함되면 스타일 매칭
        return any(kw.lower() in category_text for kw in keywords)

    # 2) 이미 사용한 spot 제외
    def usable(spots: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [s for s in spots if s["id"] not in used_spot_ids]

    all_usable = usable(all_spots)

    # 3) base_spot_id가 있으면 그 주변 후보를 먼저 가져오기
    neighbors: List[Dict[str, Any]] = []
    if base_spot_id is not None:
        neighbors = _get_neighbor_spots(
            base_spot_id=base_spot_id,
            exclude_ids=list(used_spot_ids),
            weather_mode=weather_mode,
            limit=50,
        )
    neigh_usable = usable(neighbors)

    # 4) 우선순위 리스트에 따라, 처음으로 비어있지 않은 후보군에서 random.choice
    def pick_with_priority(spots: List[Dict[str, Any]], predicates: List) -> Optional[Dict[str, Any]]:
        for pred in predicates:
            cand = [s for s in spots if pred(s)]
            if cand:
                return random.choice(cand)
        return None

    # ---------------------------
    # 1) base_spot_id가 있는 경우 (두 번째 spot 이후)
    # ---------------------------
    if base_spot_id is not None:
        # 1단계: 주변에서 스타일 매칭되는 곳 → 없으면 주변 아무 곳
        c = pick_with_priority(
            neigh_usable,
            [
                lambda s: match_style(s),
                lambda s: True,
            ],
        )
        if c:
            return c

        # 2단계: 전역에서 스타일 매칭되는 곳 → 없으면 전역 아무 곳
        c = pick_with_priority(
            all_usable,
            [
                lambda s: match_style(s),
                lambda s: True,
            ],
        )
        return c

    # ------------------------------------
    # 2) base_spot_id가 없는 경우 (첫 spot)
    # ------------------------------------
    # 비가 오든 안 오든, 첫 spot은
    # "스타일 매칭되는 곳들 중 랜덤 → 없으면 전체에서 랜덤"
    return pick_with_priority(
        all_usable,
        [
            lambda s: match_style(s),
            lambda s: True,
        ],
    )






# -------------------------------
# 날씨 모드별 스케줄 생성 함수
# -------------------------------

def generate_schedule_for_weather(
    traveler_id: int,
    weather_mode: str,          # "rainy" 또는 "not_rainy"
    schedule_pref: str | None = None,   # "빼곡한 일정 선호" / "여유로운 일정 선호" 등
) -> List[Dict[str, Any]]:
    """
    weather_mode에 따라 실내/실외/복합 관광지를 필터해서
    한 여행자(traveler_id)에 대한 N일 스케줄을 생성한다.

    - Preferred_Style: 시작 spot, 주변 spot 우선 선택에 활용
    - Preferred_Food : 음식점(밥집) 선택에 활용
    - distance_km    : 스케줄 상 모든 연속 구간(이전 장소 → 현재 장소) 거리 계산
    """
    # 1) 여행자 프로필 조회
    profile = _get_traveler_profile(traveler_id)
    if not profile:
        return []

    duration_text = profile.get("Duration") or ""    # 예: "2박 3일"
    # DB에 저장된 Schedule_Preference가 있으면 우선 사용
    schedule_pref_db = profile.get("Schedule_Preference")
    if schedule_pref_db:
        schedule_pref = schedule_pref_db
    style_pref = profile.get("Preferred_Style")
    food_pref = profile.get("Preferred_Food")

    # 2박 3일 -> 2일, 3박 4일 -> 3일 (대충 맞춰서 해석)
    nights = 1
    if "3일" in duration_text:
        nights = 2
    if "4일" in duration_text:
        nights = 3
    nights = max(1, nights)

    # 2) 날씨 모드에 맞는 전체 후보 관광지 리스트
    all_spots = _get_all_spots_for_weather(weather_mode)
    if not all_spots:
        return []

    # 3) 일정 패턴 결정
    if schedule_pref == "빼곡한 일정 선호":
        # spot → restaurant(밥집) → cafe → spot → spot → restaurant(밥집) → spot
        pattern = ["spot", "restaurant", "cafe", "spot", "spot", "restaurant", "spot"]
    else:
        # spot → restaurant(밥집) → cafe → spot → restaurant(밥집)
        pattern = ["spot", "restaurant", "cafe", "spot", "restaurant"]

    full_schedule: List[Dict[str, Any]] = []
    used_spot_ids_global: set[int] = set()
    used_rest_ids_global: set[int] = set()  # 밥집/카페 모두 공유해서 중복 방지

    # 4) day별 스케줄 구성
    for day in range(1, nights + 1):
        day_items: List[Dict[str, Any]] = []

        for order, step_type in enumerate(pattern, start=1):
            if step_type == "spot":
                # 기준이 될 base_spot_id (직전에 방문한 spot)
                base_spot_id: Optional[int] = None
                for item in reversed(day_items):
                    if item["type"] == "spot":
                        base_spot_id = item["spot_id"]
                        break

                chosen_spot = _choose_next_spot(
                    all_spots=all_spots,
                    used_spot_ids=used_spot_ids_global,
                    style_pref=style_pref,
                    weather_mode=weather_mode,
                    base_spot_id=base_spot_id,
                )
                if not chosen_spot:
                    # 더 이상 넣을 수 있는 spot이 없으면 day 루프 종료
                    break

                used_spot_ids_global.add(chosen_spot["id"])

                day_items.append(
                    {
                        "day": day,
                        "order": order,
                        "type": "spot",
                        "spot_id": chosen_spot["id"],
                        "spot_name": chosen_spot["name"],
                        "restaurant_id": None,
                        "restaurant_name": None,
                        "rating": float(chosen_spot["rating"]),
                        "distance_km": None,  # 나중에 한 번에 채움
                    }
                )

            elif step_type == "restaurant":
                # ----- 밥집(일반음식점) 추천 -----
                last_spot_id: Optional[int] = None
                for item in reversed(day_items):
                    if item["type"] == "spot":
                        last_spot_id = item["spot_id"]
                        break

                # 아직 spot이 없다면 음식점은 생략
                if last_spot_id is None:
                    continue

                food_row = _get_restaurant_for_spot(
                    spot_id=last_spot_id,
                    preferred_food=food_pref,
                    exclude_ids=list(used_rest_ids_global),
                )
                if not food_row:
                    continue

                used_rest_ids_global.add(food_row["restaurant_id"])

                day_items.append(
                    {
                        "day": day,
                        "order": order,
                        "type": "restaurant",  # 밥집
                        "spot_id": None,
                        "spot_name": None,
                        "restaurant_id": food_row["restaurant_id"],
                        "restaurant_name": food_row["restaurant_name"],
                        "rating": float(food_row["restaurant_rating"]),
                        "distance_km": None,
                    }
                )

            elif step_type == "cafe":
                # ----- 카페(휴게음식점) 추천 -----
                last_spot_id: Optional[int] = None
                # 직전에 방문한 spot 기준으로 카페 추천
                for item in reversed(day_items):
                    if item["type"] == "spot":
                        last_spot_id = item["spot_id"]
                        break

                # 아직 spot이 없다면 카페는 생략
                if last_spot_id is None:
                    continue

                cafe_row = _get_cafe_for_spot(
                    spot_id=last_spot_id,
                    exclude_ids=list(used_rest_ids_global),
                )
                if not cafe_row:
                    continue

                used_rest_ids_global.add(cafe_row["restaurant_id"])

                day_items.append(
                    {
                        "day": day,
                        "order": order,
                        "type": "cafe",
                        "spot_id": None,
                        "spot_name": None,
                        "restaurant_id": cafe_row["restaurant_id"],
                        "restaurant_name": cafe_row["restaurant_name"],
                        "rating": float(cafe_row["restaurant_rating"]),
                        "distance_km": None,
                    }
                )

        # 이 day에 대해 거리 채우기
        if day_items:
            _fill_distances_for_day(day_items)
            full_schedule.extend(day_items)

    return full_schedule



# -----------------------------------
# NOT_RAINY + RAINY 둘 다 생성 함수
# -----------------------------------

def generate_schedule_both(
    traveler_id: int,
    schedule_pref: str | None = None,
):
    """
    한 번에 맑은 날 / 비 오는 날 스케줄 둘 다 생성해서 돌려줌.
    - 두 스케줄 모두 선호(Preferred_Style / Preferred_Food) 반영
    - 스케줄 내 모든 연속 구간 거리(distance_km) 계산
    """
    not_rainy = generate_schedule_for_weather(
        traveler_id=traveler_id,
        weather_mode="not_rainy",
        schedule_pref=schedule_pref,
    )

    rainy = generate_schedule_for_weather(
        traveler_id=traveler_id,
        weather_mode="rainy",
        schedule_pref=schedule_pref,
    )

    return {
        "not_rainy": not_rainy,
        "rainy": rainy,
    }


# -----------------------------------
# Travel_Schedule 저장용 유틸
# -----------------------------------

def _insert_schedule_rows_into_db(
    traveler_id: int,
    weather_mode: str,
    schedule: List[Dict[str, Any]],
) -> int:
    """
    생성된 스케줄 리스트를 Travel_Schedule 테이블에 저장한다.
    - 기존에 같은 traveler + weather 로 저장된 스케줄은 먼저 삭제하고 다시 INSERT.
    - distance_km 는 km 단위 정수로 반올림하여 distance 컬럼에 저장.
    """
    if not schedule:
        return 0

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # 1) 기존 스케줄 삭제 (같은 여행자 + 같은 날씨)
            delete_sql = """
                DELETE FROM Travel_Schedule
                WHERE Traveler_id = %s
                  AND weather = %s
            """
            cur.execute(delete_sql, (traveler_id, weather_mode))

            # 2) 새 스케줄 삽입
            insert_sql = """
                INSERT INTO Travel_Schedule
                    (Traveler_id, Place_id, Restaurant_id,
                     visit_order, visit_date, weather, distance)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            base_date = date.today()
            inserted_count = 0

            for item in schedule:
                day_idx = int(item.get("day", 1))
                visit_date = base_date + timedelta(days=day_idx - 1)

                step_type = item["type"]
                # spot이면 Place_id 사용, 아니면 NULL
                place_id = item["spot_id"] if step_type == "spot" else None
                # restaurant / cafe 둘 다 Restaurant_id 사용
                if step_type in ("restaurant", "cafe"):
                    restaurant_id = item["restaurant_id"]
                else:
                    restaurant_id = None

                visit_order = int(item["order"])

                distance_km = item.get("distance_km")
                if distance_km is None:
                    distance_val = 0.0
                else:
                    # km 단위 정수로 저장 (예: 3.7km → 4)
                    distance_val = round(float(distance_km), 2)

                cur.execute(
                    insert_sql,
                    (
                        traveler_id,
                        place_id,
                        restaurant_id,
                        visit_order,
                        visit_date,
                        weather_mode,
                        distance_val,
                    ),
                )
                inserted_count += 1

            conn.commit()
            return inserted_count
    finally:
        conn.close()


def generate_and_save_schedule_for_traveler(
    traveler_id: int,
    weather_mode: str,
    schedule_pref: str | None = None,
) -> int:
    """
    한 여행자에 대해 특정 날씨 모드 스케줄을 생성 + Travel_Schedule 에 저장.
    반환값: INSERT 된 row 수.
    """
    schedule = generate_schedule_for_weather(
        traveler_id=traveler_id,
        weather_mode=weather_mode,
        schedule_pref=schedule_pref,
    )
    return _insert_schedule_rows_into_db(traveler_id, weather_mode, schedule)


def generate_and_save_schedule_for_all_travelers(
    schedule_pref: str | None = None,
) -> int:
    """
    TRAVELER_PROFILE 에 존재하는 모든 Traveler_ID 에 대해
    맑은 날/비 오는 날 스케줄을 전부 생성해서 Travel_Schedule 에 저장.

    반환값: 전체 INSERT 된 row 수.
    """
    # 1) 모든 Traveler_ID 조회
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT Traveler_ID FROM TRAVELER_PROFILE")
            rows = cur.fetchall()
            traveler_ids = [row["Traveler_ID"] for row in rows]
    finally:
        conn.close()

    total_inserted = 0
    for tid in traveler_ids:
        for weather_mode in ("not_rainy", "rainy"):
            schedule = generate_schedule_for_weather(
                traveler_id=tid,
                weather_mode=weather_mode,
                schedule_pref=schedule_pref,
            )
            total_inserted += _insert_schedule_rows_into_db(
                traveler_id=tid,
                weather_mode=weather_mode,
                schedule=schedule,
            )

    return total_inserted

def main():
    cnt = generate_and_save_schedule_for_all_travelers()
    print("inserted rows:", cnt)


if __name__ == "__main__":
    main()
