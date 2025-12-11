# main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

from db import get_connection
from recommend_simple import (
    recommend_spot_with_restaurant,
    recommend_for_traveler,
    generate_schedule_for_weather,
    generate_schedule_both,
)

app = FastAPI(
    title="Jeju Travel AI",
    description="서귀포 관광지 + 맛집 + 일정 추천 API",
    version="1.0.0",
)

# ------------------------------------------------------
# Pydantic 모델 정의 (요청/응답 스키마)
# ------------------------------------------------------

class SpotFoodRequest(BaseModel):
    traveler_id: int          # 현재는 limit만 사용, 나중에 traveler_id 기반 확장 가능
    limit: int = 5            # 추천할 관광지 개수


class SpotFoodItem(BaseModel):
    spot_id: int
    spot_name: str
    spot_rating: float
    restaurant_id: int | None = None
    restaurant_name: str | None = None
    restaurant_rating: float | None = None
    distance_km: float | None = None


class SpotFoodResponse(BaseModel):
    items: List[SpotFoodItem]


class ScheduleRequest(BaseModel):
    """
    일정 추천용 요청 바디

    schedule_preference:
      - "packed"  : 빼곡한 일정
      - "relaxed" : 널널한 일정
    """
    traveler_id: int
    schedule_preference: str = "packed"


# ------------------------------------------------------
# 기본 health check / DB 테스트
# ------------------------------------------------------

@app.get("/")
def root():
    return {"message": "Hello Jeju!"}


@app.get("/test-db")
def test_db():
    """DB 연결 테스트용: 관광지 한 줄만 가져오기"""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM jeju_tour_spots_info LIMIT 1;")
            row = cur.fetchone()
        conn.close()
        return {"sample_row": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ------------------------------------------------------
# 관광지 + 맛집 기본 추천 (날씨/취향 무관)
# ------------------------------------------------------

@app.post("/recommend_spot_food", response_model=SpotFoodResponse)
def recommend_spot_food(req: SpotFoodRequest):
    """
    평점 높은 관광지 + 각 관광지 주변 식당 1곳씩 묶어서 추천.
    traveler_id 는 아직 사용하지 않고, limit 개수만 사용.
    """
    try:
        items = recommend_spot_with_restaurant(limit=req.limit)
        return {"items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ------------------------------------------------------
# 여행자 프로필 기반 개인화 추천
#  - TRAVELER_PROFILE.Preferred_Food 를 활용
# ------------------------------------------------------

@app.post("/recommend_personal", response_model=SpotFoodResponse)
def recommend_personal(req: SpotFoodRequest):
    """
    TRAVELER_PROFILE 의 Preferred_Food(선호 음식 유형)을 이용해,
    선호 음식에 맞는 식당을 우선 추천한 결과를 반환.
    """
    try:
        items = recommend_for_traveler(
            traveler_id=req.traveler_id,
            limit=req.limit,
        )
        return {"items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ------------------------------------------------------
# 일정 추천 API (실내/실외 + 빼곡/널널)
#   - generate_schedule_for_weather : 특정 날씨(비 / 비안옴)에 대한 일정
#   - generate_schedule_both        : 두 가지 날씨 일정 모두 반환
# ------------------------------------------------------

@app.post("/generate_schedule_not_rainy")
def generate_schedule_not_rainy(req: ScheduleRequest):
    """
    비가 오지 않는 일반적인 날씨를 가정한 일정 추천.
    - TRAVELER_PROFILE.Duration 을 보고 2박3일 → 2일치, 3박4일 → 3일치 일정 생성
    - schedule_preference 에 따라
        * 'packed'  : 관광지-음식점-관광지-관광지-... 패턴
        * 'relaxed' : 관광지-음식점-관광지-음식점-... 패턴
    """
    try:
        # 위치 인자(순서)로만 전달: traveler_id, weather_mode, schedule_preference
        schedule = generate_schedule_for_weather(
            req.traveler_id,
            "not_rainy",
            req.schedule_preference,
        )
        return schedule
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate_schedule_rainy")
def generate_schedule_rainy(req: ScheduleRequest):
    """
    비가 오는 날(실내 + 복합 위주)을 가정한 일정 추천.
    - jeju_tour_spots_info.inout_door 컬럼을 활용
      * '실내' 또는 '복합' 위주의 관광지로 스케줄링
    """
    try:
        schedule = generate_schedule_for_weather(
            req.traveler_id,
            "rainy",
            req.schedule_preference,
        )
        return schedule
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate_schedule_both")
def generate_schedule_both_api(req: ScheduleRequest):
    """
    한 번의 호출로
      - 일반 날씨용 일정(Not Rainy)
      - 비 오는 날용 일정(Rainy)
    두 가지 스케줄을 모두 생성해서 반환.
    """
    try:
        # generate_schedule_both( traveler_id, schedule_preference )
        result = generate_schedule_both(
            req.traveler_id,
            req.schedule_preference,
        )
        # 반환 예시:
        # {
        #   "not_rainy": [...일정 리스트...],
        #   "rainy":    [...일정 리스트...]
        # }
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
