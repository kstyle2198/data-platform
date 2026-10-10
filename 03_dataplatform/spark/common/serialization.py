import math
from datetime import date, datetime
from decimal import Decimal


def normalize(value):
    """
    다양한 Python 객체를 JSON 직렬화가 가능한 자료형으로 변환한다.

    주요 처리:
    1. Spark Row 객체를 Dictionary로 변환
    2. 날짜 및 시간 객체를 문자열로 변환
    3. Decimal을 float으로 변환
    4. Dictionary 내부의 값을 재귀적으로 변환
    5. List와 Tuple 내부의 값을 재귀적으로 변환
    6. JSON 기본 자료형 및 특수한 실수 값을 처리
    7. 그 외 객체는 문자열로 변환
    """

    # --------------------------------------------------
    # 1단계: Spark Row와 같이 asDict() 메서드가 있는 객체 처리
    # --------------------------------------------------
    # Spark DataFrame의 Row 객체는 일반적인 Dictionary가 아니므로
    # asDict(recursive=True)를 사용하여 Dictionary로 변환한다.
    # recursive=True는 내부에 중첩된 Row 객체도 함께 변환한다.
    # 변환된 Dictionary는 normalize()를 다시 호출하여 추가 처리한다.
    if hasattr(value, "asDict"):
        return normalize(value.asDict(recursive=True))

    # --------------------------------------------------
    # 2단계: 날짜 및 시간 객체 처리
    # --------------------------------------------------
    # date와 datetime 객체는 JSON에서 직접 지원하지 않는다.
    # isoformat()을 사용하여 ISO 8601 형식의 문자열로 변환한다.
    # 예: date(2026, 10, 10) -> "2026-10-10"
    # 예: datetime(2026, 10, 10, 12, 30)
    #     -> "2026-10-10T12:30:00"
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    # --------------------------------------------------
    # 3단계: Decimal 객체 처리
    # --------------------------------------------------
    # Decimal은 금융 데이터나 정밀한 수치 계산에 사용될 수 있다.
    # JSON 기본 자료형으로 직접 직렬화하기 어려우므로 float으로 변환한다.
    # 주의: float 변환 시 소수점 정밀도가 손실될 수 있다.
    if isinstance(value, Decimal):
        return float(value)

    # --------------------------------------------------
    # 4단계: Dictionary 처리
    # --------------------------------------------------
    # Dictionary의 모든 키와 값을 순회한다.
    # 키는 JSON 객체에서 사용할 수 있도록 문자열로 변환하고,
    # 값은 normalize()를 다시 호출하여 내부 객체까지 재귀적으로 처리한다.
    # 예: {1: Decimal("10.5")} -> {"1": 10.5}
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}

    # --------------------------------------------------
    # 5단계: List와 Tuple 처리
    # --------------------------------------------------
    # 컬렉션 내부의 각 요소에 normalize()를 적용한다.
    # JSON 배열은 List 형태이므로 Tuple도 List로 변환한다.
    # 내부에 Dictionary, 날짜, Decimal 등이 있어도 재귀적으로 처리한다.
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]

    # --------------------------------------------------
    # 6단계: JSON 기본 자료형 및 특수한 실수 처리
    # --------------------------------------------------
    # None, 문자열, 정수, 실수, 불리언은 JSON에서 지원하는 기본 자료형이다.
    # 단, float의 NaN과 양·음의 무한대는 표준 JSON에서 허용되지 않으므로
    # math.isfinite()로 검사하여 None으로 변환한다.
    # Python의 None은 JSON으로 직렬화될 때 null이 된다.
    # 예: float("nan") -> None -> JSON null
    if value is None or isinstance(value, (str, int, float, bool)):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value

    # --------------------------------------------------
    # 7단계: 그 외 처리할 수 없는 객체
    # --------------------------------------------------
    # 위의 조건에 해당하지 않는 객체는 문자열로 변환한다.
    # JSON 직렬화 오류를 줄이는 데 도움이 되지만,
    # 원래 객체의 자료형이나 구조는 유지되지 않는다.
    return str(value)